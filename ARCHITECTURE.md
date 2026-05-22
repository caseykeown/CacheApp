# CacheApp — Architecture

## Overview

CacheApp is a local-first brain dump and cognitive organization platform. It captures unstructured text — from a web interface, an iOS app, or any future client — extracts structured tasks and ideas using local AI inference, and synchronizes everything to a cloud-backed database.

The system is built around four commitments: local-first reliability, minimal interaction friction, zero recurring infrastructure cost, and deterministic AI-assisted organization without cloud model dependency.

---

## Core Design: Event-Centric Architecture

Every piece of captured input is normalized into a single internal **Event** model before any processing or persistence occurs. There are no endpoint-specific data shapes, no per-client branching logic, and no parallel processing paths.

```
Any client input
      ↓
  ParseRequest (validated)
      ↓
   normalize_input()       ← deterministic caffeine extraction
      ↓
  enrich_event_with_llm()  ← Ollama inference
      ↓
   persist_event()         ← single Supabase write
      ↓
    Event (canonical)
```

All ingestion flows — web UI, iOS voice, future integrations — converge on this pipeline. The pipeline is implemented once in `services/event_service.py` and called from every ingestion endpoint.

---

## System Architecture

```
┌─────────────────────┐     ┌─────────────────────┐
│    Web UI (PWA)     │     │     iOS Client       │
│  static/index.html  │     │  SwiftUI + SwiftData  │
└──────────┬──────────┘     └──────────┬───────────┘
           │ HTTPS (Tailscale)         │ HTTPS (Tailscale)
           └──────────────┬────────────┘
                          ▼
             ┌────────────────────────┐
             │   FastAPI Backend      │
             │   Linux Mint Host      │
             │                        │
             │  POST /v1/events       │
             │  GET  /v1/events       │
             │  GET  /v1/events/{id}  │
             └────────┬───────────────┘
                      │
          ┌───────────┴───────────┐
          ▼                       ▼
 ┌─────────────────┐    ┌─────────────────┐
 │     Ollama      │    │    Supabase     │
 │  qwen2.5:1.5b   │    │  Postgres + API │
 │  local CPU      │    │  cloud sync     │
 └─────────────────┘    └─────────────────┘
```

---

## Event Model

All domain types live in `models/event.py`.

```python
class Event(BaseModel):
    id:             uuid.UUID       # client-generated UUIDv4 (idempotency token + PK)
    timestamp:      datetime        # client-side capture time
    source:         EventSource     # ios | web
    raw_content:    str             # verbatim input, never mutated
    resolved_type:  EventType       # brain_dump | log_caffeine | unknown | …
    metadata:       dict            # pipeline context (cleaned_text, etc.)
    derived_fields: DerivedFields   # structured output from the pipeline
```

```python
class DerivedFields(BaseModel):
    tasks:             list[ExtractedTask]
    raw_ideas:         list[str]
    mood_signal:       str | None     # positive | neutral | stressed | overwhelmed
    caffeine_items:    list[CaffeineItem]
    total_caffeine_mg: int
```

`resolved_type` is currently inferred from row content on read (caffeine-only rows become `log_caffeine`, rows with tasks or ideas become `brain_dump`). It is not yet persisted as a database column.

---

## Ingestion Pipeline

The pipeline has three stages, all implemented in `services/event_service.py`.

### Stage 1 — normalize_input()

Converts raw request fields into a partially-populated Event. No network calls.

- Parses and type-checks id, timestamp, source
- Normalizes whitespace (`raw_content` stored verbatim; `cleaned_text` stored in `metadata`)
- Runs deterministic caffeine extraction against `caffeine_db.json`:
  - Walks keys longest-first to prevent shorter substrings matching compound names
  - Blanks matched spans before continuing to prevent double-counting
- Populates `derived_fields.caffeine_items` and `derived_fields.total_caffeine_mg`
- Sets `resolved_type = unknown` (enrichment sets it)

### Stage 2 — enrich_event_with_llm()

Calls Ollama to extract structured content. One network call.

- Posts `cleaned_text` to Ollama `/api/chat` with a strict JSON schema prompt
- Temperature locked at 0 for deterministic output
- 60-second timeout (accommodates slow CPU inference)
- Strips markdown fences before JSON parsing
- Populates `derived_fields.tasks`, `raw_ideas`, `mood_signal`
- Sets `resolved_type = brain_dump`
- Raises `ValueError` on non-JSON output, `httpx.HTTPError` on transport failure
  (callers map these to 502 responses)

### Stage 3 — persist_event()

Writes the canonical record to Supabase. One network call.

- Single `INSERT` into the `tasks` table
- Derives `title` from the first extracted task (fallback: first 120 chars of raw_content)
- Writes all fields including JSONB columns: `tasks`, `raw_ideas`, `caffeine_items`
- Does not write `resolved_type` (not yet a column — future migration)

---

## API Contract

### Canonical v1 Endpoints

**POST /v1/events**
Ingest a brain dump. Returns the canonical Event object.

Request:
```json
{ "id": "<uuidv4>", "text_input": "<string>", "source": "web|ios", "timestamp": "<ISO-8601>" }
```

Response (200 OK):
```json
{ "event": { "id": "…", "timestamp": "…", "source": "web", "raw_content": "…",
             "resolved_type": "brain_dump", "metadata": {},
             "derived_fields": { "tasks": […], "raw_ideas": […],
                                 "mood_signal": "neutral",
                                 "caffeine_items": […], "total_caffeine_mg": 200 } } }
```

Duplicate (200 OK):
```json
{ "status": "duplicate", "id": "<uuid>" }
```

---

**GET /v1/events**
List events, newest-first.

Query parameters:
- `limit` — integer, default 20, max 100
- `type` — EventType value (brain_dump, log_caffeine, unknown)
- `start_time` — ISO-8601, lower bound on `created_at`
- `end_time` — ISO-8601, upper bound on `created_at`

`start_time` / `end_time` are filtered at the database level. `type` is filtered application-side (resolved_type is not a DB column yet).

Response:
```json
{ "events": [ { …Event… } ], "count": 3 }
```

---

**GET /v1/events/{event_id}**
Fetch a single event by UUID.

Response (200 OK): `{ "event": { …Event… } }`
Response (404): event not found

---

### Compatibility Endpoint (deprecated)

**POST /parse**
Preserved for existing clients during the iOS transition period.
Forwards to the identical ingestion pipeline as `POST /v1/events`.
Returns a legacy response shape (`status`, `id`, `tasks`, `raw_ideas`, `mood_signal`, `caffeine`).
Logs a deprecation warning on every call.
Will be removed once the iOS client is updated to use `/v1/events`.

---

## Idempotency

Every submission includes a client-generated UUIDv4 as the `id` field. Before running the pipeline, the backend checks whether that UUID already exists in the `tasks` table. Duplicate submissions return `200 {"status": "duplicate"}` immediately — no LLM call, no database write.

---

## Data Storage

**Supabase Postgres** is the authoritative persistent store, accessed via the service role key (bypasses RLS for all server-side operations).

Relevant table: `tasks`

| Column | Type | Notes |
|---|---|---|
| `id` | uuid | primary key, client-generated |
| `user_id` | uuid NOT NULL | owner; set to `WEB_USER_ID` from environment |
| `title` | text NOT NULL | first extracted task title or raw_content prefix |
| `source` | text | ios \| web |
| `timestamp` | text | client-side ISO-8601 capture time |
| `raw_text` | text | verbatim input |
| `tasks` | jsonb | list of ExtractedTask objects |
| `raw_ideas` | jsonb | list of non-actionable strings |
| `mood_signal` | text | positive \| neutral \| stressed \| overwhelmed |
| `caffeine_items` | jsonb | list of CaffeineItem objects |
| `total_caffeine_mg` | integer | sum of caffeine_items[].mg |
| `created_at` | timestamptz | server-assigned |

---

## Local AI Inference

**Ollama** runs on the same Linux Mint host as the backend. Model: `qwen2.5:1.5b`.

- Accessed at `OLLAMA_BASE_URL` (default: `http://localhost:11434`)
- Uses the `/api/chat` endpoint with a strict system prompt
- Temperature 0 — deterministic output across identical inputs
- 60-second timeout — accommodates Broadwell CPU inference latency
- Markdown fence stripping applied before JSON parsing
- Eliminates cloud AI dependency for core parsing

---

## Caffeine Reference Database

`caffeine_db.json` is a flat key-value map: `{ "drink name": mg_per_serving }`.
Loaded once at startup into memory. Never reloaded per-request.

The deterministic parser runs before the LLM on every ingestion, using a longest-match-first strategy so compound names ("cold brew") match before their substrings ("brew"). Matched spans are blanked to prevent double-counting.

---

## Web UI — Safari PWA

`static/index.html` is served by FastAPI at `GET /`. It is a single-file progressive web app with no build tooling, no framework dependencies, and no separate server process.

- Hosted directly by uvicorn on the same port as the API
- Tailwind CSS loaded from CDN (no local build step)
- DM Serif Display and DM Mono fonts via Google Fonts
- Posts to `POST /v1/events` with a client-generated UUIDv4
- Reads `response.event.derived_fields` for rendering
- Keyboard shortcut: Cmd/Ctrl+Enter to submit
- Accessible on any device via Tailscale HTTPS

No separate frontend server, no CORS configuration, no proxy layer required.

---

## iOS / Swift Compatibility

The v1 Event API is designed to require no backend changes when the iOS client is updated.

**Contract the iOS client must satisfy:**
- Generate UUIDv4 client-side and send as `id`
- POST to `POST /v1/events` with `{ id, text_input, source: "ios", timestamp }`
- Read the `response.event` object for persisted state
- Handle `{ status: "duplicate" }` as a successful no-op (idempotency signal)
- Use `GET /v1/events?limit=N` for pull sync (replaces individual sync routes)

**Compatibility bridge:** `POST /parse` remains active with its legacy response shape so the existing iOS client continues to function without modification until it is updated.

**SwiftData models** (`CapturedUtterance`, `TaskItem`, etc.) map cleanly to `Event.derived_fields`. No schema translation layer is required at the API boundary.

---

## Deployment

**Host:** 2015 MacBook Air (Broadwell) running Linux Mint

**Networking:** Tailscale mesh VPN. HTTPS via Tailscale-issued certificates stored in `client-web/`.

**Launch:**
```bash
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000
```

**Environment variables** (`.env` file in `backend/`):

| Variable | Required | Notes |
|---|---|---|
| `SUPABASE_URL` | yes | project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | yes | bypasses RLS for server writes |
| `WEB_USER_ID` | yes | Supabase auth UUID for the owning account |
| `OLLAMA_BASE_URL` | no | default: `http://localhost:11434` |
| `OLLAMA_MODEL` | no | default: `qwen2.5:1.5b` |

**Cost:** Zero recurring infrastructure cost. Local Ollama inference, Supabase free tier, self-managed host.

---

## Known Gaps and Future Work

**`resolved_type` not persisted** — inferred from row content on read. A future migration should add a `resolved_type` column to `tasks` and populate it at write time, enabling database-level type filtering instead of application-side post-filtering.

**`user_id` hardcoded to `WEB_USER_ID`** — single-user system. Multi-user support would require authentication middleware and per-request user resolution.

**Supabase calls are synchronous** — the supabase-py SDK is blocking. All DB calls block the uvicorn event loop. For production load, wrap with `asyncio.to_thread()` or migrate to an async Postgres client.

**iOS client uses `/parse`** — the iOS SyncWorker still targets the legacy endpoint. Once updated to call `/v1/events`, `/parse` can be hard-deleted.

**Missing table migrations** — the `tasks` table and `auth.users` setup are not captured in `supabase/migrations/`. The `20260520000000_init.sql` migration covers supporting tables only.

**Caffeine `resolved_type` inference is approximate** — a dump containing only caffeine mentions and no tasks will be typed as `log_caffeine`. A dump containing both tasks and caffeine is typed as `brain_dump`. This will be superseded once `resolved_type` is persisted.
