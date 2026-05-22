#!/usr/bin/env bash
# integrate_voice_capture.sh
#
# Run this from the ROOT of your Xcode project directory.
# It uses Claude Code CLI to wire VoiceCaptureEngine.swift into your app.
#
# Prerequisites:
#   1. Claude Code installed: npm install -g @anthropic-ai/claude-code
#   2. You are cd'd into your Xcode project root (the folder containing *.xcodeproj)
#   3. VoiceCaptureEngine.swift is downloaded and sitting at ~/Downloads/VoiceCaptureEngine.swift
#
# Usage:
#   chmod +x integrate_voice_capture.sh
#   ./integrate_voice_capture.sh

set -e

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║   Voice Capture Engine — Claude Code Integration Script      ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ── Step 1: Verify we are in an Xcode project ─────────────────────────────────
if ! ls *.xcodeproj 1>/dev/null 2>&1; then
    echo "ERROR: No .xcodeproj found in current directory."
    echo "cd into your Xcode project root and re-run this script."
    exit 1
fi

PROJECT_NAME=$(ls *.xcodeproj | sed 's/\.xcodeproj//')
echo "✓ Found Xcode project: $PROJECT_NAME"

# ── Step 2: Create the target directory ──────────────────────────────────────
TARGET_DIR="${PROJECT_NAME}/Sources/VoiceCapture"
mkdir -p "$TARGET_DIR"
echo "✓ Created directory: $TARGET_DIR"

# ── Step 3: Copy the engine file ──────────────────────────────────────────────
SOURCE_FILE=~/Downloads/VoiceCaptureEngine.swift
if [ ! -f "$SOURCE_FILE" ]; then
    echo ""
    echo "ERROR: VoiceCaptureEngine.swift not found at ~/Downloads/VoiceCaptureEngine.swift"
    echo "Move the file there and re-run, or update SOURCE_FILE path in this script."
    exit 1
fi

cp "$SOURCE_FILE" "$TARGET_DIR/VoiceCaptureEngine.swift"
echo "✓ Copied VoiceCaptureEngine.swift to $TARGET_DIR"

# ── Step 4: Run Claude Code to wire everything ────────────────────────────────
echo ""
echo "Launching Claude Code integration pass..."
echo "This will make targeted edits to your App entry point and Info.plist."
echo ""

claude --print << 'CLAUDE_PROMPT'
I have added a new file at Sources/VoiceCapture/VoiceCaptureEngine.swift to this Xcode project.
It contains: AppSession, CapturedUtterance (SwiftData model), SyncStatus enum, SyncWorker actor,
RollingSpeechManager, CaptureViewModel, and LocalCaptureView.

Please make the following precise changes to wire this into the app. Do not rewrite any file
from scratch — make surgical targeted edits only.

TASK 1 — Find the @main App struct file (the file containing `@main` and `WindowGroup`).
Add the following to the struct body if not already present:

    let container: ModelContainer = {
        let schema = Schema([CapturedUtterance.self])
        let config = ModelConfiguration(schema: schema, isStoredInMemoryOnly: false)
        return try! ModelContainer(for: schema, configurations: [config])
    }()

    init() {
        Task {
            await SyncWorker.shared.configure(with: container)
        }
        AppSession.shared.configure(
            endpoint: "http://YOUR_TAILSCALE_IP:8000",
            token: ""
        )
    }

Add `.modelContainer(container)` to the WindowGroup if not present.
Add `.environmentObject(AppSession.shared)` to the WindowGroup if not present.

TASK 2 — Find Info.plist (or the equivalent in the project's target settings).
Add these two keys if they are not already present:
    NSMicrophoneUsageDescription = "Voice capture requires microphone access to record brain dumps."
    NSSpeechRecognitionUsageDescription = "Voice capture requires speech recognition to transcribe voice input."

TASK 3 — Find the file where the user's main navigation or tab view is defined (likely
ContentView.swift or a file containing TabView or NavigationStack).
Add a tab or navigation destination for LocalCaptureView. Use this pattern:

    LocalCaptureView(context: modelContext)
        .environmentObject(AppSession.shared)

If a TabView exists, add it as a tab with label "Capture" and systemImage "mic.circle.fill".
If no TabView exists, add it as the primary view or a NavigationLink destination.

TASK 4 — Search for any existing Supabase auth handling code. If you find a sign-in
completion handler or onAuthStateChange callback, add this line inside it so the token
stays fresh in the sync worker:

    AppSession.shared.updateToken(session.accessToken)

If no auth code exists yet, add a comment placeholder:
    // TODO: Call AppSession.shared.updateToken(newToken) from your Supabase auth handler

TASK 5 — Check if the project has a background task registration (BGTaskScheduler).
If it does, add this call to the background task handler to enforce the 7-day retention limit:
    await SyncWorker.shared.enforceRetentionLimits()

If no background task exists, add a comment in the App init:
    // TODO: Register BGAppRefreshTask and call SyncWorker.shared.enforceRetentionLimits()

After all edits, list every file you modified and what you changed in each one.
Do not run the build. Do not add any Swift packages. Do not change deployment targets.
CLAUDE_PROMPT

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║   Integration complete.                                       ║"
echo "║                                                               ║"
echo "║   REQUIRED MANUAL STEP:                                       ║"
echo "║   In Xcode, right-click your target folder in the            ║"
echo "║   Project Navigator and select:                               ║"
echo "║   Add Files to \"$PROJECT_NAME\"                               ║"
echo "║   Navigate to Sources/VoiceCapture/VoiceCaptureEngine.swift  ║"
echo "║   Check: Copy items if needed = NO                           ║"
echo "║   Check: Add to target = YES (your main app target)          ║"
echo "║                                                               ║"
echo "║   Then update this line in your App struct:                   ║"
echo "║   endpoint: \"http://YOUR_TAILSCALE_IP:8000\"                  ║"
echo "║   Replace YOUR_TAILSCALE_IP with your actual Tailscale IP.   ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
