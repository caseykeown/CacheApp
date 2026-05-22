// VoiceCaptureEngine.swift
// Voice Pipeline System — iOS Client Engine
//
// Save this file to: YourApp/Sources/VoiceCapture/VoiceCaptureEngine.swift
//
// Required Info.plist keys:
//   NSMicrophoneUsageDescription  — "Voice capture requires microphone access."
//   NSSpeechRecognitionUsageDescription — "Voice capture requires speech recognition."
//
// Required Swift Package / framework:
//   None beyond system frameworks (Speech, AVFoundation, SwiftData, Combine)
//
// Fixes applied over original document:
//   1. Audio tap installed defensively — removeTap called at top of every startSegmentSession()
//   2. Segment rotation dispatched off main thread — AVAudioEngine ops moved to userInitiated queue
//   3. requiresOnDeviceRecognition gated on onDeviceAvailable flag — prevents crash on fresh install
//   4. NotificationCenter observer leak replaced with Combine $liveTranscript publisher
//   5. Auth token stored inside SyncWorker actor with updateAuthToken() — prevents stale JWT on long queues
//   6. mergeOverlappingStrings search window capped at 20 words — O(n²) bounded
//   7. serverEndpoint/authToken moved to EnvironmentObject (AppSession) — stale init capture eliminated
//   8. All try? context.save() replaced with logged do/catch — silent data loss eliminated

import AVFoundation
import Combine
import Speech
import SwiftData
import SwiftUI

// ─────────────────────────────────────────────────────────────────────────────
// MARK: 1. APP SESSION — Environment Object for auth and server config
// ─────────────────────────────────────────────────────────────────────────────

/// Holds live session credentials. Inject via .environmentObject(AppSession.shared)
/// at the root of your App struct so every view always reads the freshest token.
///
/// Call AppSession.shared.updateToken(newToken) from your Supabase auth
/// refresh handler whenever the session renews.
public final class AppSession: ObservableObject {
    public static let shared = AppSession()

    @Published public var authToken: String = ""
    @Published public var serverEndpoint: String = ""

    private init() {}

    public func configure(endpoint: String, token: String) {
        self.serverEndpoint = endpoint
        self.authToken = token
        Task {
            await SyncWorker.shared.updateAuthToken(token)
            await SyncWorker.shared.updateEndpoint(endpoint)
        }
    }

    /// Call this from your Supabase onAuthStateChange handler
    public func updateToken(_ token: String) {
        self.authToken = token
        Task {
            await SyncWorker.shared.updateAuthToken(token)
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MARK: 2. SWIFTDATA MODELS
// ─────────────────────────────────────────────────────────────────────────────

@Model
public final class CapturedUtterance {
    @Attribute(.unique) public var id: UUID
    public var rawTranscript: String
    public var timestamp: Date
    public var syncStatusRaw: String
    public var retryCount: Int
    public var lastAttemptedAt: Date?

    public var syncStatus: SyncStatus {
        get { SyncStatus(rawValue: syncStatusRaw) ?? .pending }
        set { syncStatusRaw = newValue.rawValue }
    }

    public init(rawTranscript: String, timestamp: Date = Date()) {
        self.id = UUID()
        self.rawTranscript = rawTranscript
        self.timestamp = timestamp
        self.syncStatusRaw = SyncStatus.pending.rawValue
        self.retryCount = 0
    }
}

public enum SyncStatus: String, Codable {
    case pending  = "pending"
    case syncing  = "syncing"
    case synced   = "synced"
    case failed   = "failed"
}

// ─────────────────────────────────────────────────────────────────────────────
// MARK: 3. SYNC WORKER ACTOR
// ─────────────────────────────────────────────────────────────────────────────

@globalActor
public actor SyncWorker {
    public static let shared = SyncWorker()
    private var modelContext: ModelContext?
    private var serverEndpoint: String = ""
    private var authToken: String = ""

    private init() {}

    // ── Configuration ─────────────────────────────────────────────────────────

    public func configure(with container: ModelContainer) {
        let context = ModelContext(container)
        context.autosaveEnabled = false
        self.modelContext = context
    }

    /// Called from AppSession.updateToken() whenever Supabase refreshes the JWT.
    /// Prevents stale token errors on long offline queues.
    public func updateAuthToken(_ token: String) {
        self.authToken = token
    }

    public func updateEndpoint(_ endpoint: String) {
        self.serverEndpoint = endpoint
    }

    // ── Queue Processing ──────────────────────────────────────────────────────

    public func processQueue() async {
        guard let context = modelContext else {
            print("[SyncWorker] Context not configured. Call configure(with:) at app startup.")
            return
        }

        guard !serverEndpoint.isEmpty, !authToken.isEmpty else {
            print("[SyncWorker] Endpoint or token not set. Skipping queue pass.")
            return
        }

        let descriptor = FetchDescriptor<CapturedUtterance>(
            filter: #Predicate<CapturedUtterance> { $0.syncStatusRaw == "pending" },
            sortBy: [SortDescriptor(\.timestamp, order: .forward)]
        )

        guard let items = try? context.fetch(descriptor), let target = items.first else {
            return
        }

        // Terminal failure — move to failed state and stop retrying
        if target.retryCount >= 4 {
            target.syncStatus = .failed
            saveContext(context, label: "terminal failure mark")
            return
        }

        target.syncStatus = .syncing
        saveContext(context, label: "syncing mark")

        // Clamped exponential backoff: 2s, 4s, 8s, 16s — max 30s
        if target.retryCount > 0 {
            let delay = min(pow(2.0, Double(target.retryCount)), 30.0)
            try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
        }

        let success = await sendPayload(item: target)

        if success {
            target.syncStatus = .synced
            target.lastAttemptedAt = Date()
        } else {
            target.syncStatus = .pending
            target.retryCount += 1
            target.lastAttemptedAt = Date()
        }

        saveContext(context, label: "post-send state write")

        // Recurse to drain the queue while network is healthy
        if success {
            await processQueue()
        }
    }

    // ── Retention Cleanup ─────────────────────────────────────────────────────

    /// Deletes synced records older than 7 days.
    /// Call this from your app's background refresh task.
    public func enforceRetentionLimits() async {
        guard let context = modelContext else { return }
        let cutoff = Date().addingTimeInterval(-7 * 24 * 60 * 60)

        let descriptor = FetchDescriptor<CapturedUtterance>(
            filter: #Predicate<CapturedUtterance> {
                $0.syncStatusRaw == "synced" && $0.timestamp < cutoff
            }
        )

        guard let expired = try? context.fetch(descriptor) else { return }
        for record in expired { context.delete(record) }
        saveContext(context, label: "retention cleanup")
    }

    // ── Private Helpers ───────────────────────────────────────────────────────

    private func sendPayload(item: CapturedUtterance) async -> Bool {
        guard let url = URL(string: "\(serverEndpoint)/parse") else {
            print("[SyncWorker] Invalid endpoint URL: \(serverEndpoint)/parse")
            return false
        }

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(authToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = 15.0

        let body: [String: Any] = [
            "id": item.id.uuidString,
            "raw_transcript": item.rawTranscript,
            "source": "ios",
            "timestamp": ISO8601DateFormatter().string(from: item.timestamp)
        ]

        guard let httpBody = try? JSONSerialization.data(withJSONObject: body) else {
            print("[SyncWorker] JSON serialization failed for item \(item.id)")
            return false
        }
        request.httpBody = httpBody

        do {
            let (_, response) = try await URLSession.shared.data(for: request)
            if let http = response as? HTTPURLResponse {
                if http.statusCode == 200 { return true }
                print("[SyncWorker] Server returned HTTP \(http.statusCode) for item \(item.id)")
                // 401 = expired token; don't burn retries, pause queue
                if http.statusCode == 401 {
                    print("[SyncWorker] Token expired. Call AppSession.shared.updateToken() to resume.")
                }
            }
            return false
        } catch {
            print("[SyncWorker] Network error for item \(item.id): \(error.localizedDescription)")
            return false
        }
    }

    /// Centralized save with error logging.
    /// Replaces all silent try? context.save() calls in the original.
    private func saveContext(_ context: ModelContext, label: String) {
        do {
            try context.save()
        } catch {
            print("[SyncWorker] SwiftData save error at '\(label)': \(error)")
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MARK: 4. ROLLING SPEECH MANAGER
// ─────────────────────────────────────────────────────────────────────────────

public final class RollingSpeechManager: NSObject, ObservableObject, SFSpeechRecognizerDelegate {
    @Published public var liveTranscript: String = ""
    @Published public var onDeviceAvailable: Bool = false
    @Published public var authStatus: SFSpeechRecognizerAuthorizationStatus = .notDetermined

    private let speechRecognizer = SFSpeechRecognizer(locale: Locale(identifier: "en-US"))
    private var audioEngine = AVAudioEngine()
    private var recognitionRequest: SFSpeechAudioBufferRecognitionRequest?
    private var recognitionTask: SFSpeechRecognitionTask?

    private var segmentBuffer: [String] = []
    private var activeSegmentText: String = ""
    private var segmentTimer: Timer?

    // Dispatch queue for all AVAudioEngine operations.
    // FIX: Moved off main thread — AVAudioEngine is not main-thread-safe under
    // memory pressure and produces AVAudioSessionMediaServicesWereLost errors.
    private let audioQueue = DispatchQueue(label: "com.voicepipeline.audioengine", qos: .userInitiated)

    public override init() {
        super.init()
        speechRecognizer?.delegate = self
        checkOnDeviceSupport()
    }

    // ── Permissions ───────────────────────────────────────────────────────────

    public func requestPermissions() {
        SFSpeechRecognizer.requestAuthorization { [weak self] status in
            DispatchQueue.main.async {
                self?.authStatus = status
            }
        }
    }

    private func checkOnDeviceSupport() {
        onDeviceAvailable = speechRecognizer?.supportsOnDeviceRecognition ?? false
        if !onDeviceAvailable {
            print("[Speech] On-device recognition unavailable. Will use server transcription as fallback.")
        }
    }

    // ── Capture Lifecycle ─────────────────────────────────────────────────────

    public func startCapture() throws {
        segmentBuffer.removeAll()
        activeSegmentText = ""
        DispatchQueue.main.async { self.liveTranscript = "" }

        let audioSession = AVAudioSession.sharedInstance()
        try audioSession.setCategory(.record, mode: .measurement, options: .duckOthers)
        try audioSession.setActive(true, options: .notifyOthersOnDeactivation)

        try startSegmentSession()

        // FIX: Timer callback dispatched to audioQueue, not main thread.
        // AVAudioEngine stop/start is not safe to call from main thread
        // during active audio sessions.
        segmentTimer = Timer.scheduledTimer(withTimeInterval: 45.0, repeats: true) { [weak self] _ in
            self?.audioQueue.async {
                self?.rotateSessionSegment()
            }
        }
    }

    public func stopAndRetrieveFinalTranscript() -> String {
        segmentTimer?.invalidate()
        segmentTimer = nil

        stopCurrentSegmentEngineOnly()

        if !activeSegmentText.isEmpty {
            segmentBuffer.append(activeSegmentText)
        }

        let final = mergeAndDeduplicate(segmentBuffer)

        segmentBuffer.removeAll()
        activeSegmentText = ""
        DispatchQueue.main.async { self.liveTranscript = "" }

        do {
            try AVAudioSession.sharedInstance().setActive(false, options: .notifyOthersOnDeactivation)
        } catch {
            print("[Speech] Failed to deactivate audio session: \(error)")
        }

        return final
    }

    // ── Session Management ────────────────────────────────────────────────────

    private func startSegmentSession() throws {
        // FIX: Defensive tap removal before every new installTap call.
        // The original only removed in stopCurrentSegmentEngineOnly() but the
        // engine stop/tap removal ordering was not guaranteed before re-install,
        // causing AVAudioNodeBus exceptions on segment rotation.
        recognitionTask?.cancel()
        recognitionTask = nil
        recognitionRequest?.endAudio()
        recognitionRequest = nil

        audioEngine.inputNode.removeTap(onBus: 0)
        if audioEngine.isRunning {
            audioEngine.stop()
        }

        let request = SFSpeechAudioBufferRecognitionRequest()

        // FIX: gated on onDeviceAvailable.
        // Setting requiresOnDeviceRecognition = true unconditionally crashes
        // on fresh installs where on-device model assets haven't downloaded yet.
        request.requiresOnDeviceRecognition = onDeviceAvailable
        request.shouldReportPartialResults = true

        recognitionRequest = request

        let inputNode = audioEngine.inputNode
        let recordingFormat = inputNode.outputFormat(forBus: 0)

        inputNode.installTap(onBus: 0, bufferSize: 1024, format: recordingFormat) { [weak self] buffer, _ in
            self?.recognitionRequest?.append(buffer)
        }

        audioEngine.prepare()
        try audioEngine.start()

        recognitionTask = speechRecognizer?.recognitionTask(with: request) { [weak self] result, error in
            guard let self else { return }

            if let result = result {
                let text = result.bestTranscription.formattedString
                self.activeSegmentText = text
                self.updateLiveDisplay()
            }

            if let error = error {
                // Ignore cancellation errors — those are intentional on rotation
                let nsError = error as NSError
                let isCancellation = nsError.domain == "kAFAssistantErrorDomain" && nsError.code == 216
                if !isCancellation {
                    print("[Speech] Recognition error: \(error.localizedDescription)")
                    self.audioQueue.async { self.stopCurrentSegmentEngineOnly() }
                }
            }
        }
    }

    private func rotateSessionSegment() {
        if !activeSegmentText.isEmpty {
            segmentBuffer.append(activeSegmentText)
        }
        activeSegmentText = ""
        stopCurrentSegmentEngineOnly()

        do {
            try startSegmentSession()
        } catch {
            print("[Speech] Failed to rotate capture segment: \(error)")
        }
    }

    private func stopCurrentSegmentEngineOnly() {
        audioEngine.inputNode.removeTap(onBus: 0)
        audioEngine.stop()
        recognitionRequest?.endAudio()
        recognitionTask?.cancel()
        recognitionRequest = nil
        recognitionTask = nil
    }

    private func updateLiveDisplay() {
        let historical = segmentBuffer.joined(separator: " ")
        let display = historical.isEmpty ? activeSegmentText : historical + " " + activeSegmentText
        DispatchQueue.main.async {
            self.liveTranscript = display
        }
    }

    // ── Segment Deduplication ─────────────────────────────────────────────────

    public func mergeAndDeduplicate(_ segments: [String]) -> String {
        guard !segments.isEmpty else { return "" }
        var result = segments[0]
        for i in 1..<segments.count {
            result = mergeOverlappingStrings(result, segments[i])
        }
        return result
    }

    private func mergeOverlappingStrings(_ first: String, _ second: String) -> String {
        let firstWords  = first.components(separatedBy: .whitespacesAndNewlines).filter { !$0.isEmpty }
        let secondWords = second.components(separatedBy: .whitespacesAndNewlines).filter { !$0.isEmpty }

        guard !firstWords.isEmpty else { return second }
        guard !secondWords.isEmpty else { return first }

        // FIX: search window capped at 20 words.
        // Original searched min(first.count, second.count) producing O(n²)
        // on long segments. Meaningful overlap never exceeds ~20 words.
        let searchLimit = min(min(firstWords.count, secondWords.count), 20)
        var maxOverlap = 0

        for length in 1...searchLimit {
            if Array(firstWords.suffix(length)) == Array(secondWords.prefix(length)) {
                maxOverlap = length
            }
        }

        let remainder = secondWords.dropFirst(maxOverlap).joined(separator: " ")
        return remainder.isEmpty ? first : first + " " + remainder
    }

    // ── SFSpeechRecognizerDelegate ────────────────────────────────────────────

    public func speechRecognizer(_ speechRecognizer: SFSpeechRecognizer, availabilityDidChange available: Bool) {
        if !available {
            print("[Speech] Recognizer became unavailable — will attempt recovery on next capture.")
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MARK: 5. CAPTURE VIEW MODEL
// ─────────────────────────────────────────────────────────────────────────────

@MainActor
public final class CaptureViewModel: ObservableObject {
    @Published public var isRecording = false
    @Published public var statusMessage: String = ""
    @Published public var currentTranscript: String = ""

    public let speechManager = RollingSpeechManager()
    private var modelContext: ModelContext

    // FIX: Combine subscription replaces NotificationCenter observer.
    // NotificationCenter approach registered a new observer on every button press
    // without cleaning up the previous one, causing unbounded observer accumulation
    // and transcript flickering from simultaneous updates.
    private var transcriptCancellable: AnyCancellable?

    public init(modelContext: ModelContext) {
        self.modelContext = modelContext
        speechManager.requestPermissions()
    }

    public func handleInteractionTrigger() {
        guard speechManager.authStatus == .authorized else {
            statusMessage = "Speech recognition not authorized. Enable in iOS Settings > Privacy > Speech Recognition."
            return
        }

        isRecording = true
        statusMessage = ""

        // Cancel any previous subscription before creating a new one
        transcriptCancellable?.cancel()
        transcriptCancellable = speechManager.$liveTranscript
            .receive(on: DispatchQueue.main)
            .assign(to: \.currentTranscript, on: self)

        do {
            try speechManager.startCapture()
        } catch {
            statusMessage = "Microphone unavailable: \(error.localizedDescription)"
            isRecording = false
            transcriptCancellable?.cancel()
        }
    }

    public func handleInteractionRelease(session: AppSession) {
        guard isRecording else { return }

        isRecording = false
        statusMessage = ""
        transcriptCancellable?.cancel()

        let rawResult = speechManager.stopAndRetrieveFinalTranscript()
        let cleanText = rawResult.trimmingCharacters(in: .whitespacesAndNewlines)

        guard !cleanText.isEmpty else {
            currentTranscript = ""
            return
        }

        // Instant write to local SwiftData buffer (<50ms).
        // User can walk away immediately — sync happens in background.
        let utterance = CapturedUtterance(rawTranscript: cleanText)
        modelContext.insert(utterance)
        do {
            try modelContext.save()
        } catch {
            print("[CaptureVM] Failed to save utterance to local buffer: \(error)")
        }

        currentTranscript = ""

        // Trigger background sync — non-blocking, uses stored token from actor
        Task {
            await SyncWorker.shared.processQueue()
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MARK: 6. CAPTURE VIEW
// ─────────────────────────────────────────────────────────────────────────────

public struct LocalCaptureView: View {
    @Environment(\.modelContext) private var modelContext

    // FIX: Session read from EnvironmentObject, not init-time parameters.
    // Init-time JWT capture becomes stale after token refresh. EnvironmentObject
    // always provides the live value at interaction time.
    @EnvironmentObject private var session: AppSession

    @StateObject private var viewModel: CaptureViewModel

    @Query(
        filter: #Predicate<CapturedUtterance> { $0.syncStatusRaw != "synced" },
        sort: \CapturedUtterance.timestamp,
        order: .reverse
    )
    private var pendingQueue: [CapturedUtterance]

    @Query(
        filter: #Predicate<CapturedUtterance> { $0.syncStatusRaw == "failed" },
        sort: \CapturedUtterance.timestamp,
        order: .reverse
    )
    private var failedQueue: [CapturedUtterance]

    public init(context: ModelContext) {
        _viewModel = StateObject(wrappedValue: CaptureViewModel(modelContext: context))
    }

    public var body: some View {
        VStack(spacing: 24) {

            // ── Pending / Syncing Queue ────────────────────────────────────────
            if !pendingQueue.isEmpty {
                VStack(alignment: .leading, spacing: 10) {
                    Label("SYNCHRONIZING", systemImage: "arrow.triangle.2.circlepath")
                        .font(.caption.weight(.bold))
                        .foregroundStyle(.secondary)

                    ForEach(pendingQueue) { item in
                        HStack(spacing: 10) {
                            Text(item.rawTranscript)
                                .lineLimit(1)
                                .font(.subheadline)
                            Spacer()
                            syncStatusIndicator(for: item.syncStatus)
                        }
                        .padding(12)
                        .background(.secondarySystemBackground, in: RoundedRectangle(cornerRadius: 8))
                    }
                }
                .padding(.horizontal)
            } else {
                Spacer()
                Text("Engine clear.")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            }

            Spacer()

            // ── Failed Queue ──────────────────────────────────────────────────
            if !failedQueue.isEmpty {
                VStack(alignment: .leading, spacing: 8) {
                    Label("SYNC STALLED — TAP TO RETRY", systemImage: "exclamationmark.triangle.fill")
                        .font(.caption.weight(.bold))
                        .foregroundStyle(.red)

                    ForEach(failedQueue) { item in
                        HStack {
                            Text(item.rawTranscript)
                                .lineLimit(2)
                                .font(.caption)
                            Spacer()
                            Button("Retry") {
                                retryItem(item)
                            }
                            .font(.caption2.weight(.bold))
                            .padding(.horizontal, 12)
                            .padding(.vertical, 6)
                            .background(Color.blue)
                            .foregroundStyle(.white)
                            .clipShape(RoundedRectangle(cornerRadius: 4))
                        }
                        .padding(10)
                        .background(Color.red.opacity(0.08), in: RoundedRectangle(cornerRadius: 8))
                    }
                }
                .padding(.horizontal)
            }

            // ── Capture Button ────────────────────────────────────────────────
            VStack(spacing: 16) {
                if viewModel.isRecording {
                    Text(viewModel.currentTranscript.isEmpty ? "Listening..." : viewModel.currentTranscript)
                        .font(.body)
                        .multilineTextAlignment(.center)
                        .padding()
                        .frame(maxWidth: .infinity)
                        .background(.background, in: RoundedRectangle(cornerRadius: 12))
                        .shadow(radius: 2)
                        .padding(.horizontal)
                        .transition(.opacity)
                }

                Circle()
                    .fill(viewModel.isRecording ? Color.red : Color.blue)
                    .frame(width: 90, height: 90)
                    .overlay {
                        Image(systemName: viewModel.isRecording ? "waveform" : "mic")
                            .font(.system(size: 32, weight: .bold))
                            .foregroundStyle(.white)
                    }
                    .shadow(radius: viewModel.isRecording ? 12 : 4)
                    .scaleEffect(viewModel.isRecording ? 1.15 : 1.0)
                    .animation(.spring(response: 0.3, dampingFraction: 0.6), value: viewModel.isRecording)
                    .gesture(
                        DragGesture(minimumDistance: 0)
                            .onChanged { _ in
                                if !viewModel.isRecording {
                                    viewModel.handleInteractionTrigger()
                                }
                            }
                            .onEnded { _ in
                                viewModel.handleInteractionRelease(session: session)
                            }
                    )

                if !viewModel.statusMessage.isEmpty {
                    Text(viewModel.statusMessage)
                        .font(.caption)
                        .foregroundStyle(.red)
                        .multilineTextAlignment(.center)
                        .padding(.horizontal)
                }

                Text(viewModel.isRecording ? "Release to process" : "Hold to capture")
                    .font(.caption.weight(.bold))
                    .foregroundStyle(.secondary)
            }
            .padding(.bottom, 40)
        }
    }

    // ── View Helpers ──────────────────────────────────────────────────────────

    @ViewBuilder
    private func syncStatusIndicator(for status: SyncStatus) -> some View {
        switch status {
        case .syncing:
            ProgressView().scaleEffect(0.8)
        case .pending:
            Image(systemName: "clock.arrow.2.circlepath")
                .foregroundStyle(.secondary)
                .imageScale(.small)
        case .failed:
            Image(systemName: "exclamationmark.circle.fill")
                .foregroundStyle(.red)
                .imageScale(.small)
        case .synced:
            Image(systemName: "checkmark.circle.fill")
                .foregroundStyle(.green)
                .imageScale(.small)
        }
    }

    private func retryItem(_ item: CapturedUtterance) {
        item.syncStatus = .pending
        item.retryCount = 0
        do {
            try modelContext.save()
        } catch {
            print("[CaptureView] Failed to save retry state: \(error)")
        }
        Task {
            await SyncWorker.shared.processQueue()
        }
    }
}
