"""
Brain Dump Ingestion API
POST /parse  — accepts iOS + Web payloads, deduplicates via Supabase,
               runs deterministic caffeine pre-parsing, then hits Ollama
               (Qwen 2.5) for structured JSON extraction.

Dependencies:
    pip install fastapi uvicorn httpx supabase python-dotenv

Env vars (or .env file):
    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY   (bypasses RLS for server-side writes)
    OLLAMA_BASE_URL      (default: http://localhost:11434)
    OLLAMA_MODEL         (default: qwen2.5:1.5b)
"""

from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, field_validator
from supabase import Client, create_client

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

# Compound-input split pattern: "coffee + red bull", "coffee, red bull"
_COMPOUND_SEP = re.compile(r"\s*(?:\+|,|&|and)\s*", re.IGNORECASE)


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
# Deterministic caffeine parsing
# ---------------------------------------------------------------------------
def extract_caffeine_items(text: str) -> list[dict[str, Any]]:
    """
    Scans the raw text for caffeine-related phrases.
    Handles compound inputs like 'coffee + Red Bull' or
    'had a coffee and a Monster this morning'.

    Walks DB keys longest-first so compound names ("cold brew") match before
    their substrings ("brew"). Blanks matched spans to prevent re-matching.
    """
    text_lower = text.lower()
    found: list[dict[str, Any]] = []

    for name in sorted(CAFFEINE_DB.keys(), key=len, reverse=True):
        if name in text_lower:
            found.append({"name": name, "mg": CAFFEINE_DB[name]})
            # Blank matched span so shorter patterns don't re-match
            text_lower = text_lower.replace(name, " " * len(name), 1)

    return found


def preprocess_text(text: str) -> tuple[str, list[dict[str, Any]]]:
    """
    Strips and lightly normalises the input, then extracts caffeine items
    deterministically before the LLM pass.

    Returns (cleaned_text, caffeine_items).
    """
    cleaned = text.strip()
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    caffeine_items = extract_caffeine_items(cleaned)
    return cleaned, caffeine_items


# ---------------------------------------------------------------------------
# Ollama integration
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


async def call_ollama(text: str) -> dict[str, Any]:
    payload = {
        "model": OLLAMA_MODEL,
        "stream": False,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": text},
        ],
    }
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(
            f"{OLLAMA_BASE_URL}/api/chat",
            json=payload,
        )
    resp.raise_for_status()
    raw = resp.json()["message"]["content"].strip()

    # Strip accidental markdown fences if the model slips
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Ollama returned non-JSON: {raw[:200]}") from exc


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
# Canonical Supabase write
# ---------------------------------------------------------------------------
def write_to_supabase(
    task_id: str,
    source: str,
    timestamp: str,
    raw_text: str,
    parsed: dict[str, Any],
    caffeine: list[dict[str, Any]],
) -> None:
    tasks = parsed.get("tasks", [])
    # title is NOT NULL — use first extracted task title, fall back to raw_text prefix
    title = (tasks[0].get("title") if tasks else None) or raw_text[:120]
    record = {
        "id": task_id,
        "user_id": WEB_USER_ID,
        "title": title,
        "source": source,
        "timestamp": timestamp,
        "raw_text": raw_text,
        "tasks": tasks,
        "raw_ideas": parsed.get("raw_ideas", []),
        "mood_signal": parsed.get("mood_signal"),
        "caffeine_items": caffeine,
        "total_caffeine_mg": sum(c["mg"] for c in caffeine),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    supabase.table("tasks").insert(record).execute()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/")
async def serve_index():
    index = Path(__file__).parent / "static" / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return JSONResponse({"status": "Brain Dump API running. Place index.html in ./static/"})


@app.post("/parse")
async def parse_brain_dump(payload: ParseRequest):
    # 1. Idempotency guard — duplicate UUIDs return immediately, no LLM call
    if is_duplicate(payload.id):
        return JSONResponse(
            status_code=200,
            content={"status": "duplicate", "id": payload.id},
        )

    # 2. Deterministic pre-processing + caffeine extraction
    cleaned_text, caffeine_items = preprocess_text(payload.text_input)

    # 3. LLM extraction
    try:
        parsed = await call_ollama(cleaned_text)
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(status_code=502, detail=f"LLM error: {exc}")

    # 4. Canonical write — single insert with all data
    try:
        write_to_supabase(
            task_id=payload.id,
            source=payload.source.value,
            timestamp=payload.timestamp,
            raw_text=payload.text_input,
            parsed=parsed,
            caffeine=caffeine_items,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Supabase write failed: {exc}")

    # 5. Return structured result to client
    return JSONResponse(
        status_code=200,
        content={
            "status": "ok",
            "id": payload.id,
            "tasks": parsed.get("tasks", []),
            "raw_ideas": parsed.get("raw_ideas", []),
            "mood_signal": parsed.get("mood_signal"),
            "caffeine": {
                "items": caffeine_items,
                "total_mg": sum(c["mg"] for c in caffeine_items),
            },
        },
    )


@app.get("/health")
async def health():
    return {"status": "ok", "model": OLLAMA_MODEL}
