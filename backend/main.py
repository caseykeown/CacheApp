"""
Brain Dump Ingestion API

Endpoints:
  POST /parse            — legacy ingestion (iOS + web); delegates to _ingest()
  POST /v1/events        — versioned ingestion; delegates to _ingest()
  GET  /v1/events        — unified read: latest N events, with optional filters
                           ?limit=     max records (default 20, cap 100)
                           ?type=      EventType value (brain_dump|log_caffeine|…)
                           ?start_time= ISO-8601 lower bound on created_at
                           ?end_time=   ISO-8601 upper bound on created_at
  GET  /health           — liveness check

All ingestion logic lives in services/event_service.py.
All domain types live in models/event.py.

Dependencies:
    pip install fastapi uvicorn httpx supabase python-dotenv

Env vars (or .env file):
    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY   (bypasses RLS for server-side writes)
    OLLAMA_BASE_URL             (default: http://localhost:11434)
    OLLAMA_MODEL                (default: qwen2.5:1.5b)
    WEB_USER_ID                 (Supabase auth UUID for the owning account)
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from enum import Enum
from pathlib import Path

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, field_validator
from supabase import Client, create_client

from services import enrich_event_with_llm, fetch_events, normalize_input, persist_event

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SUPABASE_URL: str = os.environ["SUPABASE_URL"]
# Service role key bypasses RLS — correct for server-side writes
SUPABASE_KEY: str = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
OLLAMA_BASE_URL: str = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL: str = os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b")
# Single-user system — Supabase auth UUID for the account that owns all rows
WEB_USER_ID: str = os.environ["WEB_USER_ID"]
CAFFEINE_DB_PATH: Path = Path(__file__).parent / "caffeine_db.json"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
app = FastAPI(title="Brain Dump API")

# Serve static assets (CSS, JS, images) from ./static/
_STATIC_DIR = Path(__file__).parent / "static"
if _STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


# ---------------------------------------------------------------------------
# Caffeine DB  (loaded once at startup)
# ---------------------------------------------------------------------------
def _load_caffeine_db() -> dict[str, int]:
    """
    Expected format: { "coffee": 95, "red bull": 80, ... }
    All keys lowercase.
    """
    if CAFFEINE_DB_PATH.exists():
        with CAFFEINE_DB_PATH.open() as f:
            return {k.lower(): v for k, v in json.load(f).items()}
    # Sensible fallback so the server starts without the file
    return {
        "coffee": 95,
        "espresso": 63,
        "double espresso": 126,
        "americano": 95,
        "latte": 75,
        "cappuccino": 75,
        "cold brew": 200,
        "red bull": 80,
        "monster": 160,
        "celsius": 200,
        "bang": 300,
        "rockstar": 160,
        "5 hour energy": 200,
        "pre workout": 150,
        "tea": 47,
        "green tea": 35,
        "matcha": 70,
        "diet coke": 46,
        "coke": 34,
    }


CAFFEINE_DB: dict[str, int] = _load_caffeine_db()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
class SourceEnum(str, Enum):
    ios = "ios"
    web = "web"


class ParseRequest(BaseModel):
    id: str
    text_input: str
    source: SourceEnum
    timestamp: str  # ISO-8601

    @field_validator("id")
    @classmethod
    def validate_uuid(cls, v: str) -> str:
        try:
            uuid.UUID(v)
        except ValueError:
            raise ValueError("id must be a valid UUID v4")
        return v

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("timestamp must be ISO-8601")
        return v


# ---------------------------------------------------------------------------
# Idempotency guard
# ---------------------------------------------------------------------------
def is_duplicate(task_id: str) -> bool:
    result = (
        supabase.table("tasks")
        .select("id")
        .eq("id", task_id)
        .limit(1)
        .execute()
    )
    return len(result.data) > 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_iso(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid ISO-8601 datetime: {value!r}",
        )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/")
async def serve_index():
    index = Path(__file__).parent / "static" / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return JSONResponse({"status": "Brain Dump API running. Place index.html in ./static/"})


async def _ingest(payload: ParseRequest) -> JSONResponse:
    """Shared pipeline for POST /parse and POST /v1/events."""
    if is_duplicate(payload.id):
        return JSONResponse(
            status_code=200,
            content={"status": "duplicate", "id": payload.id},
        )

    event = normalize_input(
        request_id=payload.id,
        text_input=payload.text_input,
        source=payload.source.value,
        timestamp=payload.timestamp,
        caffeine_db=CAFFEINE_DB,
    )

    try:
        event = await enrich_event_with_llm(event, OLLAMA_BASE_URL, OLLAMA_MODEL)
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(status_code=502, detail=f"LLM error: {exc}")

    try:
        persist_event(event, supabase, WEB_USER_ID)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Supabase write failed: {exc}")

    return JSONResponse(
        status_code=200,
        content={
            "status": "ok",
            "id": str(event.id),
            "tasks": [t.model_dump() for t in event.derived_fields.tasks],
            "raw_ideas": event.derived_fields.raw_ideas,
            "mood_signal": event.derived_fields.mood_signal,
            "caffeine": {
                "items": [c.model_dump() for c in event.derived_fields.caffeine_items],
                "total_mg": event.derived_fields.total_caffeine_mg,
            },
        },
    )


@app.post("/parse")
async def parse_brain_dump(payload: ParseRequest):
    return await _ingest(payload)


@app.post("/v1/events")
async def ingest_event(payload: ParseRequest):
    return await _ingest(payload)


@app.get("/v1/events")
async def list_events(
    limit: int = 20,
    event_type: str | None = Query(None, alias="type"),
    start_time: str | None = None,
    end_time: str | None = None,
):
    start_dt = _parse_iso(start_time) if start_time else None
    end_dt   = _parse_iso(end_time)   if end_time   else None
    try:
        events = fetch_events(
            supabase, WEB_USER_ID, limit,
            event_type=event_type,
            start_time=start_dt,
            end_time=end_dt,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Supabase read failed: {exc}")
    return JSONResponse(
        status_code=200,
        content={
            "events": [e.model_dump(mode="json") for e in events],
            "count": len(events),
        },
    )


@app.get("/health")
async def health():
    return {"status": "ok", "model": OLLAMA_MODEL}
