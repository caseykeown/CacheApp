"""
Event — unified internal domain model for every inbound ingestion event.

All ingestion paths (iOS voice, web text dump) resolve to this type.
Nothing outside this module should define a competing representation of
what a captured input *is* — the pipeline builds one of these, persists
it, and returns a projection of it to the caller.
"""

from __future__ import annotations

import uuid as uuid_module
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Source — where the event originated
# ---------------------------------------------------------------------------

class EventSource(str, Enum):
    ios = "ios"
    web = "web"


# ---------------------------------------------------------------------------
# EventType — what the pipeline resolved the input to be
# ---------------------------------------------------------------------------

class EventType(str, Enum):
    # Multi-intent extraction (current web UI flow: tasks + ideas + caffeine
    # all pulled from a single brain dump in one pass)
    brain_dump = "brain_dump"

    # Single-intent routes (iOS voice commands and targeted web inputs)
    log_task       = "log_task"
    append_case    = "append_case"
    log_caffeine   = "log_caffeine"
    log_medication = "log_medication"

    # Pipeline could not classify the input
    unknown = "unknown"


# ---------------------------------------------------------------------------
# DerivedFields — structured output produced by the processing pipeline
# ---------------------------------------------------------------------------

class ExtractedTask(BaseModel):
    """A single actionable item extracted from the raw input."""
    title: str
    urgency: str = "medium"                           # high | medium | low
    focus_tags: list[str] = Field(default_factory=list)
    notes: str | None = None


class CaffeineItem(BaseModel):
    """A single caffeine source identified by deterministic pre-processing."""
    name: str
    mg: int


class DerivedFields(BaseModel):
    """
    Everything the pipeline produces from the raw input.

    Populated in two stages:
      1. Deterministic pre-processing (caffeine_items, total_caffeine_mg)
      2. LLM inference (tasks, raw_ideas, mood_signal)

    Default-empty so an Event can be constructed before the pipeline runs.
    """
    tasks: list[ExtractedTask] = Field(default_factory=list)
    raw_ideas: list[str] = Field(default_factory=list)
    mood_signal: str | None = None                    # positive | neutral | stressed | overwhelmed
    caffeine_items: list[CaffeineItem] = Field(default_factory=list)
    total_caffeine_mg: int = 0


# ---------------------------------------------------------------------------
# Event — the canonical domain object
# ---------------------------------------------------------------------------

class Event(BaseModel):
    """
    Single internal representation of a captured input event.

    Fields
    ------
    id              Client-generated UUIDv4. Used as the idempotency token
                    and the primary key in Supabase.

    timestamp       Client-side capture time. Preserved as-is; server
                    assigns its own created_at separately.

    source          Originating client (ios | web). Determines auth path,
                    routing defaults, and sync behaviour.

    raw_content     Verbatim text as received. Never mutated after creation.

    resolved_type   What the pipeline determined this event to be. Defaults
                    to `unknown` until the intent router runs.

    metadata        Arbitrary context that varies by source and session:
                    user_id, focus_mode, correction_rules, applied_corrections,
                    auth claims, etc. Kept flexible so callers can attach
                    whatever the current pipeline stage needs without
                    polluting the core fields.

    derived_fields  Typed output from deterministic pre-processing and LLM
                    inference. Default-empty so an Event can exist before
                    the pipeline has run.
    """

    id: uuid_module.UUID
    timestamp: datetime
    source: EventSource
    raw_content: str
    resolved_type: EventType = EventType.unknown
    metadata: dict[str, Any] = Field(default_factory=dict)
    derived_fields: DerivedFields = Field(default_factory=DerivedFields)

    model_config = {"frozen": False}
