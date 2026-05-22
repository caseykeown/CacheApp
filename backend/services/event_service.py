"""
services/event_service.py — event normalization, LLM enrichment, persistence,
                             and read-path reconstruction.

Write path (three stages):
  normalize_input()       raw request fields  →  partially-populated Event
  enrich_event_with_llm() Event + Ollama      →  fully-populated Event
  persist_event()         Event + Supabase    →  row in tasks table

Read path:
  fetch_events()          Supabase            →  list[Event]
"""

from __future__ import annotations

import json
import re
import uuid as uuid_module
from datetime import datetime, timezone
from typing import Any

import httpx
from supabase import Client

from models import (
    CaffeineItem,
    DerivedFields,
    Event,
    EventSource,
    EventType,
    ExtractedTask,
)


# ---------------------------------------------------------------------------
# LLM prompt
# Canonical definition. main.py carries a duplicate until it is wired to
# call enrich_event_with_llm(); after that, this is the only copy.
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are a strict JSON extraction engine. Given an unstructured brain dump, \
extract structured data and return ONLY valid JSON with no prose, no markdown, \
no code fences, and no explanation.

Output schema (all fields required, use null when unknown):
{
  "tasks": [
    {
      "title": "<concise action phrase, max 10 words>",
      "urgency": "<high|medium|low>",
      "focus_tags": ["<tag>"],
      "notes": "<optional clarifying detail or null>"
    }
  ],
  "raw_ideas": ["<non-actionable thought or observation>"],
  "mood_signal": "<positive|neutral|stressed|overwhelmed|null>"
}

Rules:
- Do NOT invent dates, deadlines, or specific times unless explicitly stated.
- Do NOT split one task into multiple unless the user named them separately.
- Urgency defaults to "medium" when no signal is present.
- focus_tags should be 1-3 lowercase single-word labels (e.g. health, work, finance, family).
- raw_ideas captures observations, worries, half-thoughts that are not action items.
- Return ONLY the JSON object. Nothing else.
"""


# ---------------------------------------------------------------------------
# Stage 1 — normalize
# ---------------------------------------------------------------------------

def normalize_input(
    request_id: str,
    text_input: str,
    source: str,
    timestamp: str,
    caffeine_db: dict[str, int],
) -> Event:
    """
    Convert raw /parse request fields into a partially-populated Event.

    Runs deterministic caffeine pre-processing and stores the
    whitespace-normalised text in metadata["cleaned_text"] for the
    LLM enrichment step. resolved_type remains unknown until
    enrich_event_with_llm() runs.

    Matches the behaviour of main.py's preprocess_text() exactly so
    swapping in this function produces identical pipeline output.
    """
    # Mirror preprocess_text() whitespace normalisation
    cleaned = re.sub(r"\s{2,}", " ", text_input.strip())

    caffeine_items = _extract_caffeine_items(cleaned, caffeine_db)

    return Event(
        id=uuid_module.UUID(request_id),
        timestamp=datetime.fromisoformat(timestamp.replace("Z", "+00:00")),
        source=EventSource(source),
        raw_content=text_input,
        # cleaned_text lives in metadata — it is pipeline context, not
        # core content. raw_content is always the verbatim original.
        metadata={"cleaned_text": cleaned},
        derived_fields=DerivedFields(
            caffeine_items=caffeine_items,
            total_caffeine_mg=sum(c.mg for c in caffeine_items),
        ),
    )


def _extract_caffeine_items(text: str, caffeine_db: dict[str, int]) -> list[CaffeineItem]:
    """
    Walk caffeine_db keys longest-first so compound names ("cold brew")
    match before their substrings ("brew"). Blank matched spans so shorter
    overlapping keys don't re-match the same text.
    """
    text_lower = text.lower()
    found: list[CaffeineItem] = []

    for name in sorted(caffeine_db.keys(), key=len, reverse=True):
        if name in text_lower:
            found.append(CaffeineItem(name=name, mg=caffeine_db[name]))
            text_lower = text_lower.replace(name, " " * len(name), 1)

    return found


# ---------------------------------------------------------------------------
# Stage 2 — LLM enrichment
# ---------------------------------------------------------------------------

async def enrich_event_with_llm(
    event: Event,
    ollama_base_url: str,
    ollama_model: str,
) -> Event:
    """
    Call Ollama to extract tasks, ideas, and mood from the event content.

    Uses metadata["cleaned_text"] as the LLM input (the whitespace-normalised
    form set by normalize_input). Falls back to raw_content if not present.

    Merges LLM output into event.derived_fields, preserving caffeine items
    already populated by normalize_input. Sets resolved_type to brain_dump.

    Raises:
      ValueError        — Ollama returned non-JSON output.
      httpx.HTTPError   — transport or non-2xx response from Ollama.

    Callers are responsible for mapping these to HTTP responses.
    Mutates the event in place and returns it so callers can chain.
    """
    llm_input = event.metadata.get("cleaned_text", event.raw_content)

    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(
            f"{ollama_base_url}/api/chat",
            json={
                "model": ollama_model,
                "stream": False,
                "messages": [
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": llm_input},
                ],
            },
        )
    resp.raise_for_status()

    raw = resp.json()["message"]["content"].strip()
    # Strip markdown fences if the model slips
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        parsed: dict[str, Any] = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Ollama returned non-JSON: {raw[:200]}") from exc

    tasks = [
        ExtractedTask(
            title=t.get("title", ""),
            urgency=t.get("urgency", "medium"),
            focus_tags=t.get("focus_tags") or [],
            notes=t.get("notes"),
        )
        for t in parsed.get("tasks") or []
        if t.get("title")  # drop any task the model returned without a title
    ]

    # Rebuild derived_fields, carrying forward caffeine data from stage 1
    event.derived_fields = DerivedFields(
        tasks=tasks,
        raw_ideas=parsed.get("raw_ideas") or [],
        mood_signal=parsed.get("mood_signal"),
        caffeine_items=event.derived_fields.caffeine_items,
        total_caffeine_mg=event.derived_fields.total_caffeine_mg,
    )
    event.resolved_type = EventType.brain_dump
    return event


# ---------------------------------------------------------------------------
# Stage 3 — persistence
# ---------------------------------------------------------------------------

def persist_event(
    event: Event,
    supabase_client: Client,
    user_id: str,
) -> None:
    """
    Write the canonical event record to the Supabase tasks table.

    Produces the exact same row shape as main.py's write_to_supabase() so
    the database schema requires no changes when this replaces the direct
    write. title is derived from the first extracted task, falling back to
    the first 120 chars of raw_content to satisfy the NOT NULL constraint.
    """
    tasks_raw = [t.model_dump() for t in event.derived_fields.tasks]
    title = (tasks_raw[0].get("title") if tasks_raw else None) or event.raw_content[:120]

    supabase_client.table("tasks").insert({
        "id":               str(event.id),
        "user_id":          user_id,
        "title":            title,
        "source":           event.source.value,
        "timestamp":        event.timestamp.isoformat(),
        "raw_text":         event.raw_content,
        "tasks":            tasks_raw,
        "raw_ideas":        event.derived_fields.raw_ideas,
        "mood_signal":      event.derived_fields.mood_signal,
        "caffeine_items":   [c.model_dump() for c in event.derived_fields.caffeine_items],
        "total_caffeine_mg": event.derived_fields.total_caffeine_mg,
        "created_at":       datetime.now(timezone.utc).isoformat(),
    }).execute()


# ---------------------------------------------------------------------------
# Read path
# ---------------------------------------------------------------------------

_FALLBACK_TS = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _row_to_event(row: dict[str, Any]) -> Event:
    """
    Reconstruct an Event from a Supabase tasks-table row.

    Defensive against None / missing fields — rows written by older backend
    versions may lack caffeine_items, raw_ideas, or a parseable timestamp.
    """
    # timestamp was stored as an ISO string from the client; fall back to
    # created_at (server-assigned) so the field is always populated
    raw_ts = row.get("timestamp") or row.get("created_at") or ""
    try:
        ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        ts = _FALLBACK_TS

    try:
        source = EventSource(row.get("source") or "web")
    except ValueError:
        source = EventSource.web

    tasks = [
        ExtractedTask(
            title=t.get("title", ""),
            urgency=t.get("urgency", "medium"),
            focus_tags=t.get("focus_tags") or [],
            notes=t.get("notes"),
        )
        for t in (row.get("tasks") or [])
        if t.get("title")
    ]

    caffeine_items = [
        CaffeineItem(name=c.get("name", ""), mg=int(c.get("mg", 0)))
        for c in (row.get("caffeine_items") or [])
        if c.get("name")
    ]

    return Event(
        id=uuid_module.UUID(str(row["id"])),
        timestamp=ts,
        source=source,
        raw_content=row.get("raw_text") or "",
        resolved_type=_infer_type(row),
        derived_fields=DerivedFields(
            tasks=tasks,
            raw_ideas=list(row.get("raw_ideas") or []),
            mood_signal=row.get("mood_signal"),
            caffeine_items=caffeine_items,
            total_caffeine_mg=int(row.get("total_caffeine_mg") or 0),
        ),
    )


def _infer_type(row: dict[str, Any]) -> EventType:
    """
    Infer EventType from row content.

    resolved_type is not persisted as a column yet, so we derive it:
      caffeine items present, no tasks/ideas  →  log_caffeine
      tasks or raw ideas present              →  brain_dump
      otherwise                               →  unknown
    """
    has_caffeine = bool(row.get("caffeine_items"))
    has_tasks    = bool(row.get("tasks"))
    has_ideas    = bool(row.get("raw_ideas"))

    if has_caffeine and not has_tasks and not has_ideas:
        return EventType.log_caffeine
    if has_tasks or has_ideas:
        return EventType.brain_dump
    return EventType.unknown


def fetch_events(
    supabase_client: Client,
    user_id: str,
    limit: int = 20,
    event_type: str | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> list[Event]:
    """
    Return events for the given user, newest-first.

    start_time / end_time filter on created_at at the database level.
    event_type filters application-side (resolved_type is not a DB column).
    When a type filter is active the query fetches the full 100-row cap
    before filtering so the caller receives up to `limit` typed results.

    Hard cap: 100 rows per call regardless of limit.
    """
    limit = min(max(limit, 1), 100)
    # Fetch the full cap when type-filtering to avoid under-returning after
    # the application-side filter discards non-matching rows.
    fetch_limit = 100 if event_type else limit

    query = (
        supabase_client.table("tasks")
        .select(
            "id, source, timestamp, raw_text, tasks, raw_ideas, "
            "mood_signal, caffeine_items, total_caffeine_mg, created_at"
        )
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .limit(fetch_limit)
    )
    if start_time:
        query = query.gte("created_at", start_time.isoformat())
    if end_time:
        query = query.lte("created_at", end_time.isoformat())

    events = [_row_to_event(row) for row in query.execute().data]

    if event_type:
        try:
            target = EventType(event_type)
            events = [e for e in events if e.resolved_type == target]
        except ValueError:
            pass  # unrecognised type value — return all rather than erroring

    return events[:limit]
