# Cache App — Architecture

## Overview

Cache App is a local-first voice capture and cognitive organization platform. It handles rapid brain dump ingestion, structured task extraction, lightweight health tracking, and contextual focus management.

The system is built on four principles: local-first reliability, minimal interaction friction, zero recurring infrastructure cost, and AI-assisted organization without cloud dependency.

The stack is a Linux Mint server running local LLM inference through Ollama, a SwiftUI iOS client for primary capture workflows, Supabase as the synchronization and authentication layer, and a lightweight web interface for remote testing and administration.

---

## System Architecture

```
┌─────────────────────┐
│     iOS Client      │
│  SwiftUI + SwiftData│
└─────────┬───────────┘
          │ HTTPS + JWT
          ▼
┌─────────────────────┐
│   FastAPI Backend   │
│   Linux Mint Host   │
│  Ollama + Routing   │
└─────────┬───────────┘
          │ PostgREST / Supabase SDK
          ▼
┌─────────────────────┐
│      Supabase       │
│ Postgres + Auth API │
└─────────────────────┘
          ▲
          │ HTTPS
          ▼
┌─────────────────────┐
│     Web UI Layer    │
└─────────────────────┘
```

---

## Backend (`backend/`)

FastAPI application hosted on the Linux Mint server, exposed over Tailscale.

**Runtime:** Python 3.12+, FastAPI, Uvicorn

**AI Inference:** Ollama with `qwen2.5:1.5b` as the default model. Temperature is locked at 0 for deterministic output. Request timeout is 60 seconds.

**Storage:** Supabase Postgres for persistent cloud sync. Local JSON datasets for static reference data (e.g., caffeine database). In-memory caches for transient performance.

**Static Hosting:** The backend mounts a `/static` directory registered as a catch-all route after all API routes are defined. The web UI is served from here.

**Responsibilities:** Voice and text ingestion, intent extraction, context-aware classification, sync orchestration, focus state management, daily readings ingestion, health tracking logic, correction learning pipeline.

**Known Gap:** Focus state is stored in-process and does not survive backend restarts.

---

## iOS Client (`client-ios/CacheApp/`)

Primary user-facing application. Built with SwiftUI and SwiftData targeting iOS 17+.

**Responsibilities:** Voice capture, local persistence, offline queuing, authentication, background sync, health tracking, focus workflows.

**Key Modules:**

| Module | Responsibility |
|---|---|
| `AuthManager.swift` | Supabase OAuth session management |
| `VoiceCaptureEngine.swift` | Speech recognition and ingestion |
| `Models.swift` | SwiftData schema and sync actors |
| `SupabaseManager.swift` | API client wrapper |
| `ContentView.swift` | Root navigation |
| `CasesAndHealth.swift` | Case and health interfaces |
| `Tasks/` | Task management UI |

**SwiftData Models:** `CapturedUtterance`, `TaskItem`, `CaffeineItemModel`, `USCCBReadingModel`, `PipelineRunModel`, `FocusStateAuditModel`

The client is designed to remain fully operational during network outages. Pending uploads persist locally and sync automatically when connectivity is restored.

---

## Web UI (`client-web/`)

A single-file browser interface served by the FastAPI static mount over HTTPS via Tailscale certificates.

**Features:** Health monitoring, focus state visibility, manual text payload submission, JWT-based endpoint testing, Web Speech API capture.

**Primary purpose:** Remote diagnostics, backend validation, and endpoint testing. It is not a replacement for the iOS client.

**Known Gap:** JWT authentication state is not persisted between sessions.

---

## Voice Capture Engine

Implemented as a reusable Swift module using `SFSpeechRecognizer` with rolling 45-second recognition windows.

The engine supports incremental transcript capture, local-first persistence, async sync queue processing, and retry handling with exponential backoff.

---

## Data Flow

### Voice Ingestion Pipeline

```
iOS Microphone
    ↓
RollingSpeechManager
    ↓
CaptureViewModel
    ↓
SwiftData Persistence
    ↓
SyncWorker Queue
    ↓
POST /parse
    ↓
JWT Validation
    ↓
Context + Preferences Fetch
    ↓
Deterministic Pre-processing (regex, caffeine_db.json)
    ↓
Ollama Inference
    ↓
Intent Router
    ↓
Supabase Persistence
```

### Web Ingestion Pipeline

```
Browser (index.html)
    ↓
Client-side UUIDv4 generation
    ↓
POST /parse  { id, text_input, source: "web", timestamp }
    ↓
Idempotency check (Supabase tasks table)
    ↓
Deterministic Pre-processing
    ↓
Ollama Inference
    ↓
Supabase Persistence
    ↓
200 OK → UI state update
```

---

## Intent Routing

The parser routes each input to one of the following intents:

| Intent | Action |
|---|---|
| `log_task` | Create structured task |
| `append_case` | Append investigative or case note |
| `log_caffeine` | Create caffeine log entry |
| `log_medication` | Create medication log entry |
| `unknown` | Return unclassified payload |

---

## Authentication

Authentication runs through Supabase Auth using GitHub OAuth.

```
iOS Client
    ↓
Supabase OAuth
    ↓
GitHub Authentication
    ↓
JWT Issuance
    ↓
Backend Validation (supabase.auth.get_user(token))
    ↓
Authorized API Access
```

The iOS client automatically propagates refreshed JWTs through `AuthManager` and shared session state. The web client requires manual JWT entry.

---

## Database

Supabase Postgres is the authoritative persistent datastore.

**Stable tables (migrations exist):**

- `caffeine_items`
- `usccb_readings`
- `pipeline_runs`
- `focus_state_audit`

**Required operational tables (must exist before production):**

- `tasks`
- `cases`
- `health_logs`
- `corrections`
- `user_preferences`
- `focus_states`

---

## API Surface

| Endpoint | Purpose |
|---|---|
| `GET /health` | System health and diagnostics |
| `POST /parse` | Primary ingestion endpoint (iOS + web) |
| `POST /correct` | User correction ingestion |
| `GET/POST /focus/*` | Focus state management |
| `GET /readings/*` | Daily liturgical readings |
| `GET /caffeine/*` | Caffeine dataset access |
| `GET/POST /pipelines/*` | Pipeline orchestration |

`POST /parse` accepts: `id` (UUIDv4), `text_input` (string), `source` (enum: `ios` | `web`), `timestamp` (ISO 8601).

Idempotency is enforced by checking the `tasks` table for the submitted UUID before processing. Duplicate submissions return `200 OK` without re-executing the pipeline.

---

## Pull Synchronization

The iOS client periodically pulls: caffeine datasets, daily readings, pipeline history, and focus state audits. Synchronization runs through an actor-isolated `SyncEngine`.

**Known Gap:** `SyncEngine` initialization is incomplete, blocking pull synchronization.

---

## Local-First Design

Voice capture succeeds offline. Pending uploads persist locally. Sync retries automatically. Local Ollama inference eliminates cloud AI dependency for core parsing.

---

## AI Strategy

Primary categorization and parsing run locally through Ollama at `temperature=0`. The model acts as a strict JSON formatting engine; deterministic regex pre-processing handles known patterns (compound caffeine inputs, comma-separated lists) before the LLM is invoked. This keeps inference fast, cheap, and predictable.

Complex reasoning may optionally escalate to larger hosted models in future releases.

---

## Known Architectural Gaps

**Missing runtime table migrations** — several parser-dependent tables are not yet in migrations.

**Incomplete idempotency** — duplicate protection currently applies only to task ingestion. Cases, health logs, and corrections are unprotected.

**SyncEngine initialization** — incomplete configuration blocks pull synchronization from running.

**Retention scheduler** — background retention tasks are implemented but not registered with the process lifecycle.

**Web JWT persistence** — the web client does not persist authentication state across page loads.

**Caffeine dataset disk reads** — `caffeine_db.json` is reloaded from disk on every request instead of cached in memory at startup.

**Raw transcript durability** — raw utterances are stored locally only and are not persisted server-side.

---

## Deployment

**Host:** 2015 MacBook Air running Linux Mint

**Networking:** Tailscale mesh VPN with HTTPS via Tailscale certificates

**Cost model:** Zero recurring infrastructure cost. Ollama for local inference, Supabase free tier for sync, self-managed hosting.

---

## Definition of Done

A release is considered production-ready when:

- Voice capture is stable and low-friction
- AI categorization is reliable and deterministic
- Health tracking workflows are fully persistent
- Daily readings ingestion is stable
- Sync survives restart and network interruption
- All required database migrations exist and are applied
- All ingestion flows are idempotent (tasks, cases, health logs, corrections)
- UI remains uncluttered and accessible
- Architecture documentation reflects implementation reality
- Data is portable and exportable
