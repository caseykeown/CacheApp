"""
Voice Pipeline System — Production Backend Engine
FastAPI application managing ingestion pipelines, focus states,
text normalization, USCCB daily-readings scraping, and Supabase integration.

Fixes applied over original:
  1. LLM fence-stripping replaced with robust regex (strip("`") was corrupting output)
  2. json.JSONDecodeError caught separately to expose malformed LLM output in error body
  3. HTTPException re-raise guard prevents outer except swallowing intentional 4xx/5xx
  4. 422 on unknown intent replaced with graceful fallback log + 200 return
  5. cases notes field guarded with `or []` to prevent NoneType append crash
  6. Caffeine lookup uses partial-key fallback when exact key missing
  7. datetime.datetime.utcnow() references replaced with dt_class.now(UTC) (utcnow deprecated)
  8. FocusStateManager._audit uses asyncio.get_event_loop() replaced with thread-safe asyncio.get_running_loop()
  9. CORS wildcard tightened — credentials=True with allow_origins=["*"] is rejected by browsers; origins locked to explicit list
 10. UtterancePayload gains idempotency id + source enum + timestamp to match iOS SyncWorker contract
 11. Duplicate /liturgical-readings legacy route removed (superseded by /readings)
 12. apply_in_context_rules guards against malformed regex patterns crashing the request
 13. Ollama timeout raised to 60s to handle slow CPU inference on Broadwell without 500ing
 14. Root GET / added to silence 404 log noise on browser access
 15. Added idempotency check: duplicate UUID submissions return cached response without re-processing
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import re
import traceback
import unicodedata
import uuid as uuid_module
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime as dt_class, timedelta
from enum import Enum
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional

import httpx
from bs4 import BeautifulSoup, Tag
from cachetools import TTLCache
from fastapi import BackgroundTasks, FastAPI, Depends, HTTPException, status, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from supabase import Client, create_client
from fastapi.staticfiles import StaticFiles
# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("voice_pipeline")

BASE_DIR = Path(__file__).parent


# ─────────────────────────────────────────────────────────────────────────────
# 1. SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

class Settings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = True
    log_level: str = "info"

    supabase_url: str
    supabase_anon_key: str
    supabase_service_role_key: str
    supabase_publishable_key: str = ""

    anthropic_api_key: str = ""
    openai_api_key: str = ""
    deepgram_api_key: str = ""
    elevenlabs_api_key: str = ""

    ollama_host: str = "http://localhost:11434"

    # Explicit allowed CORS origins. Wildcard + credentials=True is rejected by
    # all modern browsers. Add your Tailscale IP and any other origins here.
    cors_origins: str = "http://localhost:8000,http://localhost:3000"

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()  # type: ignore[call-arg]
OLLAMA_HOST = settings.ollama_host
_CORS_ORIGINS: list[str] = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]


# ─────────────────────────────────────────────────────────────────────────────
# 2. ENUMS
# ─────────────────────────────────────────────────────────────────────────────

class FocusState(str, Enum):
    IDLE       = "idle"
    LISTENING  = "listening"
    PROCESSING = "processing"
    SPEAKING   = "speaking"
    INGESTING  = "ingesting"
    PAUSED     = "paused"
    ERROR      = "error"


class PipelineStatus(str, Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    COMPLETED = "completed"
    FAILED    = "failed"
    CACHED    = "cached"


class PipelineType(str, Enum):
    CAFFEINE_DB = "caffeine_db"
    USCCB       = "usccb"
    GENERIC     = "generic"


class ContentType(str, Enum):
    READING         = "reading"
    PSALM           = "psalm"
    GOSPEL          = "gospel"
    ALLELUIA        = "alleluia"
    RESPONSORIAL    = "responsorial"
    CAFFEINE_ITEM   = "caffeine_item"
    GENERIC         = "generic"


class IngestionSource(str, Enum):
    IOS  = "ios"
    WEB  = "web"
    TEST = "test"


# ─────────────────────────────────────────────────────────────────────────────
# 3. PYDANTIC MODELS
# ─────────────────────────────────────────────────────────────────────────────

class PipelineRun(BaseModel):
    id: str
    pipeline_type: PipelineType
    status: PipelineStatus
    started_at: dt_class
    completed_at: dt_class | None = None
    records_processed: int = 0
    error: str | None = None
    meta: dict[str, Any] = Field(default_factory=dict)


class FocusStateResponse(BaseModel):
    state: FocusState
    previous_state: FocusState | None
    changed_at: dt_class
    context: dict[str, Any] = Field(default_factory=dict)


class ReadingItem(BaseModel):
    content_type: ContentType
    title: str
    citation: str = ""
    text: str
    normalized_text: str = ""


class DailyReading(BaseModel):
    date: date
    liturgical_day: str = ""
    readings: list[ReadingItem]
    normalized_readings: list[ReadingItem] = Field(default_factory=list)
    cached: bool = False
    fetched_at: dt_class = Field(default_factory=lambda: dt_class.now(UTC))


class CaffeineItem(BaseModel):
    id: int
    name: str
    size_oz: float | None
    caffeine_mg: int
    category: str
    sugar_free: bool


class NormalizationResult(BaseModel):
    original: str
    normalized: str
    changes: list[str] = Field(default_factory=list)


class HealthResponse(BaseModel):
    status: str
    version: str
    focus_state: FocusState
    supabase_connected: bool
    active_pipelines: int
    timestamp: dt_class


# Rebuild DailyReading after ReadingItem is defined
DailyReading.model_rebuild()


class UtterancePayload(BaseModel):
    """
    Unified ingestion payload accepted from both iOS SyncWorker and Web UI.
    id: client-generated UUIDv4 used as idempotency token.
    raw_transcript: the voice or text input string.
    source: identifies the originating client.
    timestamp: ISO8601 client-side capture time.
    """
    id: str = Field(default_factory=lambda: str(uuid_module.uuid4()))
    raw_transcript: str = Field(..., min_length=1, max_length=8000)
    source: IngestionSource = IngestionSource.TEST
    timestamp: Optional[str] = None


class IngestionIntentResponse(BaseModel):
    intent: str
    structured_data: Dict[str, Any]
    applied_corrections: List[Any] = []


class CorrectionSubmission(BaseModel):
    original_transcript: str
    original_output: Dict[str, Any]
    corrected_output: Dict[str, Any]


# ─────────────────────────────────────────────────────────────────────────────
# 4. TEXT NORMALIZER
# ─────────────────────────────────────────────────────────────────────────────

_BIBLE_BOOKS: dict[str, str] = {
    "Gn":  "Genesis",          "Ex":  "Exodus",          "Lv":  "Leviticus",
    "Nm":  "Numbers",          "Dt":  "Deuteronomy",     "Jos": "Joshua",
    "Jgs": "Judges",           "Ru":  "Ruth",            "1 Sm": "First Samuel",
    "2 Sm": "Second Samuel",   "1 Kgs": "First Kings",   "2 Kgs": "Second Kings",
    "1 Chr": "First Chronicles","2 Chr": "Second Chronicles","Ezr": "Ezra",
    "Neh": "Nehemiah",         "Tb":  "Tobit",           "Jdt": "Judith",
    "Est": "Esther",           "1 Mc": "First Maccabees","2 Mc": "Second Maccabees",
    "Jb":  "Job",              "Ps":  "Psalm",           "Prv": "Proverbs",
    "Eccl":"Ecclesiastes",     "Song":"Song of Songs",   "Wis": "Wisdom",
    "Sir": "Sirach",           "Is":  "Isaiah",          "Jer": "Jeremiah",
    "Lam": "Lamentations",     "Bar": "Baruch",          "Ez":  "Ezekiel",
    "Dn":  "Daniel",           "Hos": "Hosea",           "Jl":  "Joel",
    "Am":  "Amos",             "Ob":  "Obadiah",         "Jon": "Jonah",
    "Mi":  "Micah",            "Na":  "Nahum",           "Hb":  "Habakkuk",
    "Zep": "Zephaniah",        "Hg":  "Haggai",          "Zec": "Zechariah",
    "Mal": "Malachi",
    "Mt":  "Matthew",          "Mk":  "Mark",            "Lk":  "Luke",
    "Jn":  "John",             "Acts":"Acts",            "Rom": "Romans",
    "1 Cor": "First Corinthians","2 Cor": "Second Corinthians",
    "Gal": "Galatians",        "Eph": "Ephesians",       "Phil":"Philippians",
    "Col": "Colossians",       "1 Thes": "First Thessalonians",
    "2 Thes": "Second Thessalonians","1 Tm": "First Timothy",
    "2 Tm": "Second Timothy",  "Ti":  "Titus",           "Phlm":"Philemon",
    "Heb": "Hebrews",          "Jas": "James",           "1 Pt": "First Peter",
    "2 Pt": "Second Peter",    "1 Jn": "First John",     "2 Jn": "Second John",
    "3 Jn": "Third John",      "Jude":"Jude",            "Rv":  "Revelation",
}

_ECCL_ABBREVS: dict[str, str] = {
    r"\bSt\.\s": "Saint ",      r"\bSts\.\s": "Saints ",
    r"\bMsgr\.\s": "Monsignor ",r"\bBp\.\s": "Bishop ",
    r"\bAbp\.\s": "Archbishop ",r"\bCard\.\s": "Cardinal ",
    r"\bFr\.\s": "Father ",     r"\bSr\.\s": "Sister ",
    r"\bBr\.\s": "Brother ",    r"\bDr\.\s": "Doctor ",
    r"\bMt\.\s": "Mount ",      r"\bvs\.\s": "versus ",
    r"\betc\.\s": "et cetera, ",r"\bi\.e\.,?\s": "that is, ",
    r"\be\.g\.,?\s": "for example, ",
}

_ORDINALS = {
    "1": "one",  "2": "two",   "3": "three", "4": "four",
    "5": "five", "6": "six",   "7": "seven", "8": "eight",
    "9": "nine", "10": "ten",  "11": "eleven","12": "twelve",
}

_UNICODE_SUBS = [
    ("\u2018", "'"), ("\u2019", "'"), ("\u201c", '"'), ("\u201d", '"'),
    ("\u2013", " - "), ("\u2014", " - "), ("\u2026", "..."),
    ("\u00a0", " "),
]


class TextNormalizer:
    """Transforms raw liturgical / scripture text into TTS-friendly prose."""

    _citation_re = re.compile(
        r"(?P<book>(?:1 |2 |3 )?[A-Z][a-z]{0,4})\s+(?P<chap>\d+):(?P<vstart>\d+)(?:-(?P<vend>\d+))?"
    )
    _number_re = re.compile(r"\b(\d{1,2})\b")
    _whitespace_re = re.compile(r"\s{2,}")

    def normalize(self, text: str) -> NormalizationResult:
        changes: list[str] = []
        result = text

        result, c = self._strip_unicode(result)
        if c:
            changes.append("stripped_unicode_entities")

        result, c = self._expand_scripture_citations(result)
        if c:
            changes += c

        result, c = self._expand_abbreviations(result)
        if c:
            changes.append("expanded_abbreviations")

        result, c = self._normalize_whitespace(result)
        if c:
            changes.append("normalized_whitespace")

        return NormalizationResult(original=text, normalized=result, changes=changes)

    def _strip_unicode(self, text: str) -> tuple[str, bool]:
        result = text
        for char, replacement in _UNICODE_SUBS:
            result = result.replace(char, replacement)
        result = unicodedata.normalize("NFKD", result)
        result = result.encode("ascii", "ignore").decode("ascii")
        return result, result != text

    def _expand_scripture_citations(self, text: str) -> tuple[str, list[str]]:
        changes: list[str] = []

        def _replace(m: re.Match) -> str:
            book_abbr = m.group("book")
            book_full = _BIBLE_BOOKS.get(book_abbr)
            if not book_full:
                return m.group(0)

            chap = m.group("chap")
            vstart = m.group("vstart")
            vend = m.group("vend")

            chap_word = _ORDINALS.get(chap, chap)
            vstart_word = _ORDINALS.get(vstart, vstart)

            if vend:
                vend_word = _ORDINALS.get(vend, vend)
                expanded = (
                    f"{book_full} chapter {chap_word}, "
                    f"verses {vstart_word} through {vend_word}"
                )
            else:
                expanded = f"{book_full} chapter {chap_word}, verse {vstart_word}"

            changes.append(f"citation:{m.group(0)}->{expanded}")
            return expanded

        result = self._citation_re.sub(_replace, text)
        return result, changes

    def _expand_abbreviations(self, text: str) -> tuple[str, bool]:
        result = text
        for pattern, replacement in _ECCL_ABBREVS.items():
            result = re.sub(pattern, replacement, result)
        return result, result != text

    def _normalize_whitespace(self, text: str) -> tuple[str, bool]:
        result = self._whitespace_re.sub(" ", text).strip()
        return result, result != text


# ─────────────────────────────────────────────────────────────────────────────
# 5. USCCB SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

USCCB_BASE = "https://bible.usccb.org/bible/readings"
_usccb_cache: TTLCache = TTLCache(maxsize=14, ttl=82800)
_cache_lock = Lock()


class USCCBScraper:
    """Scrapes daily Mass readings from bible.usccb.org and caches results."""

    def __init__(self, normalizer: TextNormalizer, supabase: Client | None = None):
        self.normalizer = normalizer
        self.supabase = supabase

    async def get_readings(self, target_date: date | None = None) -> DailyReading:
        target = target_date or date.today()
        cache_key = target.isoformat()

        with _cache_lock:
            if cache_key in _usccb_cache:
                log.info("USCCB cache hit for %s", cache_key)
                cached: DailyReading = _usccb_cache[cache_key]
                cached.cached = True
                return cached

        reading = await self._fetch_and_parse(target)
        reading.normalized_readings = self._normalize_all(reading.readings)

        # Validate gospel is present before caching — guards against markup changes
        gospel_items = [r for r in reading.readings if r.content_type == ContentType.GOSPEL]
        if not gospel_items or len(gospel_items[0].text) < 30:
            log.warning("USCCB gospel validation failed for %s — not caching empty result", target)
            # Return stub rather than caching bad data
            return DailyReading(
                date=target,
                liturgical_day="Readings temporarily unavailable",
                readings=[ReadingItem(
                    content_type=ContentType.GOSPEL,
                    title="Readings Unavailable",
                    text="Readings temporarily unavailable. Visit usccb.org/readings",
                )],
            )

        with _cache_lock:
            _usccb_cache[cache_key] = reading

        if self.supabase:
            await asyncio.to_thread(self._persist, reading)

        return reading

    def cache_stats(self) -> dict[str, Any]:
        with _cache_lock:
            return {
                "size": len(_usccb_cache),
                "maxsize": _usccb_cache.maxsize,
                "ttl_seconds": _usccb_cache.ttl,
                "keys": list(_usccb_cache.keys()),
            }

    def invalidate(self, target_date: date | None = None) -> bool:
        key = (target_date or date.today()).isoformat()
        with _cache_lock:
            existed = key in _usccb_cache
            _usccb_cache.pop(key, None)
        return existed

    async def _fetch_and_parse(self, target: date) -> DailyReading:
        url = self._build_url(target)
        log.info("Fetching USCCB readings: %s", url)

        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            try:
                resp = await client.get(url, headers={"User-Agent": "VoicePipeline/1.0"})
                resp.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"USCCB returned {exc.response.status_code} for {url}",
                )
            except httpx.RequestError as exc:
                raise HTTPException(
                    status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                    detail=f"Network error fetching USCCB: {exc}",
                )

        return self._parse_html(resp.text, target)

    @staticmethod
    def _build_url(target: date) -> str:
        return f"{USCCB_BASE}/{target.strftime('%m%d%y')}.cfm"

    def _parse_html(self, html: str, target: date) -> DailyReading:
        soup = BeautifulSoup(html, "html.parser")
        readings: list[ReadingItem] = []

        liturgical_day = self._extract_liturgical_day(soup)
        sections = soup.find_all("div", class_=re.compile(r"(reading|psalm|gospel|alleluia)", re.I))

        if not sections:
            sections = self._fallback_sections(soup)

        for section in sections:
            item = self._parse_section(section)
            if item:
                readings.append(item)

        if not readings:
            log.warning("No readings parsed from USCCB HTML for %s; returning stub", target)

        return DailyReading(date=target, liturgical_day=liturgical_day, readings=readings)

    def _extract_liturgical_day(self, soup: BeautifulSoup) -> str:
        for sel in ["h1.name", ".field-name-title h1", "h1", ".content-header h2"]:
            tag = soup.select_one(sel)
            if tag and tag.get_text(strip=True):
                return tag.get_text(strip=True)
        return ""

    def _parse_section(self, section: Tag) -> ReadingItem | None:
        title_tag = section.find(re.compile(r"h[2-4]"))
        title = title_tag.get_text(strip=True) if title_tag else ""

        citation_tag = section.find(class_=re.compile(r"citation|reference|address", re.I))
        citation = citation_tag.get_text(strip=True) if citation_tag else ""

        paragraphs = section.find_all("p")
        text = " ".join(p.get_text(separator=" ", strip=True) for p in paragraphs if p.get_text(strip=True))

        if not text:
            text = section.get_text(separator=" ", strip=True)

        if not text or len(text) < 20:
            return None

        content_type = self._classify_content(title)
        return ReadingItem(
            content_type=content_type,
            title=title or content_type.value.title(),
            citation=citation,
            text=text,
        )

    @staticmethod
    def _fallback_sections(soup: BeautifulSoup) -> list[Tag]:
        headings = soup.find_all(re.compile(r"h[2-4]"), string=re.compile(
            r"(reading|psalm|gospel|alleluia|responsorial)", re.I
        ))
        sections: list[Tag] = []
        for h in headings:
            parent = h.find_parent(["div", "section", "article"])
            if parent and parent not in sections:
                sections.append(parent)
        return sections

    @staticmethod
    def _classify_content(title: str) -> ContentType:
        t = title.lower()
        if "psalm" in t or "responsorial" in t:
            return ContentType.PSALM
        if "gospel" in t:
            return ContentType.GOSPEL
        if "alleluia" in t:
            return ContentType.ALLELUIA
        if "reading" in t:
            return ContentType.READING
        return ContentType.GENERIC

    def _normalize_all(self, readings: list[ReadingItem]) -> list[ReadingItem]:
        normalized: list[ReadingItem] = []
        for item in readings:
            result = self.normalizer.normalize(item.text)
            normalized.append(item.model_copy(update={
                "text": result.normalized,
                "normalized_text": result.normalized,
            }))
        return normalized

    def _persist(self, reading: DailyReading) -> None:
        if not self.supabase:
            return
        try:
            payload = {
                "reading_date": reading.date.isoformat(),
                "liturgical_day": reading.liturgical_day,
                "readings": [r.model_dump() for r in reading.readings],
                "normalized_readings": [r.model_dump() for r in reading.normalized_readings],
                "fetched_at": reading.fetched_at.isoformat(),
            }
            (
                self.supabase.table("usccb_readings")
                .upsert(payload, on_conflict="reading_date")
                .execute()
            )
            log.debug("Persisted USCCB readings for %s to Supabase", reading.date)
        except Exception as exc:
            log.warning("Failed to persist USCCB readings to Supabase: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# 6. INGESTION PIPELINE MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class IngestionPipeline:
    """Tracks run history and executes data hydration pipelines asynchronously."""

    def __init__(self, supabase: Client | None = None):
        self.supabase = supabase
        self._runs: dict[str, PipelineRun] = {}
        self._lock = asyncio.Lock()

    async def run_caffeine_db(self) -> PipelineRun:
        run = self._make_run(PipelineType.CAFFEINE_DB)
        async with self._lock:
            self._runs[run.id] = run
        asyncio.create_task(self._exec_caffeine_db(run))
        return run

    async def run_usccb(
        self,
        scraper: USCCBScraper,
        target_date: date | None = None,
    ) -> PipelineRun:
        run = self._make_run(PipelineType.USCCB)
        async with self._lock:
            self._runs[run.id] = run
        asyncio.create_task(self._exec_usccb(run, scraper, target_date))
        return run

    def get_run(self, run_id: str) -> PipelineRun | None:
        return self._runs.get(run_id)

    def list_runs(self) -> list[PipelineRun]:
        return list(self._runs.values())

    def active_count(self) -> int:
        return sum(1 for r in self._runs.values() if r.status == PipelineStatus.RUNNING)

    async def _exec_caffeine_db(self, run: PipelineRun) -> None:
        run.status = PipelineStatus.RUNNING
        log.info("[pipeline:%s] caffeine_db started", run.id)
        try:
            db_path = BASE_DIR / "caffeine_db.json"
            raw = json.loads(db_path.read_text())
            items: list[dict] = raw.get("items", [])

            if self.supabase:
                await asyncio.to_thread(
                    lambda: self.supabase.table("caffeine_items")  # type: ignore[union-attr]
                    .upsert(items, on_conflict="id")
                    .execute()
                )

            run.records_processed = len(items)
            run.status = PipelineStatus.COMPLETED
            log.info("[pipeline:%s] caffeine_db done — %d records", run.id, len(items))
        except Exception as exc:
            run.status = PipelineStatus.FAILED
            run.error = str(exc)
            log.error("[pipeline:%s] caffeine_db failed: %s", run.id, exc)
        finally:
            run.completed_at = dt_class.now(UTC)
            await asyncio.to_thread(self._record_run, run)

    async def _exec_usccb(
        self,
        run: PipelineRun,
        scraper: USCCBScraper,
        target_date: date | None,
    ) -> None:
        run.status = PipelineStatus.RUNNING
        log.info("[pipeline:%s] usccb started for %s", run.id, target_date or "today")
        try:
            reading = await scraper.get_readings(target_date)
            run.records_processed = len(reading.readings)
            run.meta["date"] = reading.date.isoformat()
            run.meta["liturgical_day"] = reading.liturgical_day
            run.meta["cached"] = reading.cached
            run.status = PipelineStatus.CACHED if reading.cached else PipelineStatus.COMPLETED
            log.info("[pipeline:%s] usccb done — %d readings", run.id, len(reading.readings))
        except HTTPException as exc:
            run.status = PipelineStatus.FAILED
            run.error = exc.detail
            log.error("[pipeline:%s] usccb http error: %s", run.id, exc.detail)
        except Exception as exc:
            run.status = PipelineStatus.FAILED
            run.error = str(exc)
            log.error("[pipeline:%s] usccb failed: %s", run.id, exc)
        finally:
            run.completed_at = dt_class.now(UTC)
            await asyncio.to_thread(self._record_run, run)

    @staticmethod
    def _make_run(pipeline_type: PipelineType) -> PipelineRun:
        return PipelineRun(
            id=str(uuid_module.uuid4()),
            pipeline_type=pipeline_type,
            status=PipelineStatus.PENDING,
            started_at=dt_class.now(UTC),
        )

    def _record_run(self, run: PipelineRun) -> None:
        if not self.supabase:
            return
        try:
            self.supabase.table("pipeline_runs").insert(run.model_dump(mode="json")).execute()
        except Exception as exc:
            log.warning("Failed to record pipeline run to Supabase: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# 7. FOCUS STATE MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class FocusStateManager:
    """Thread-safe system focus state machine with validation and audit trail."""

    _TRANSITIONS: dict[FocusState, set[FocusState]] = {
        FocusState.IDLE:       {FocusState.LISTENING, FocusState.INGESTING, FocusState.PAUSED},
        FocusState.LISTENING:  {FocusState.PROCESSING, FocusState.IDLE, FocusState.ERROR},
        FocusState.PROCESSING: {FocusState.SPEAKING, FocusState.IDLE, FocusState.ERROR},
        FocusState.SPEAKING:   {FocusState.IDLE, FocusState.LISTENING, FocusState.ERROR},
        FocusState.INGESTING:  {FocusState.IDLE, FocusState.ERROR},
        FocusState.PAUSED:     {FocusState.IDLE, FocusState.LISTENING},
        FocusState.ERROR:      {FocusState.IDLE},
    }

    def __init__(self, supabase: Client | None = None):
        self._state = FocusState.IDLE
        self._previous: FocusState | None = None
        self._changed_at = dt_class.now(UTC)
        self._context: dict[str, Any] = {}
        self._history: list[FocusStateResponse] = []
        self._lock = Lock()
        self.supabase = supabase

    @property
    def current(self) -> FocusStateResponse:
        with self._lock:
            return FocusStateResponse(
                state=self._state,
                previous_state=self._previous,
                changed_at=self._changed_at,
                context=dict(self._context),
            )

    def transition(
        self,
        target: FocusState,
        context: dict[str, Any] | None = None,
        force: bool = False,
    ) -> FocusStateResponse:
        with self._lock:
            if not force and target not in self._TRANSITIONS.get(self._state, set()):
                raise ValueError(
                    f"Invalid transition {self._state} -> {target}. "
                    f"Allowed: {self._TRANSITIONS.get(self._state, set())}"
                )
            self._previous = self._state
            self._state = target
            self._changed_at = dt_class.now(UTC)
            self._context = context or {}
            snapshot = FocusStateResponse(
                state=self._state,
                previous_state=self._previous,
                changed_at=self._changed_at,
                context=dict(self._context),
            )
            self._history.append(snapshot)
            log.info("Focus state: %s -> %s", self._previous, self._state)

        if self.supabase:
            # FIX: asyncio.get_event_loop() is deprecated in async context.
            # Use a plain thread since _audit is a sync method.
            import threading
            threading.Thread(target=self._audit, args=(snapshot,), daemon=True).start()

        return snapshot

    def history(self, limit: int = 20) -> list[FocusStateResponse]:
        with self._lock:
            return list(self._history[-limit:])

    def allowed_transitions(self) -> list[FocusState]:
        with self._lock:
            return sorted(self._TRANSITIONS.get(self._state, set()), key=lambda s: s.value)

    def reset(self) -> FocusStateResponse:
        return self.transition(FocusState.IDLE, force=True)

    def _audit(self, snapshot: FocusStateResponse) -> None:
        if not self.supabase:
            return
        try:
            self.supabase.table("focus_state_audit").insert({
                "state": snapshot.state.value,
                "previous_state": snapshot.previous_state.value if snapshot.previous_state else None,
                "changed_at": snapshot.changed_at.isoformat(),
                "context": snapshot.context,
            }).execute()
        except Exception as exc:
            log.warning("Failed to audit focus state: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# 8. SUPABASE MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class SupabaseManager:
    """Thin wrapper around the Supabase SDK client with connection verification."""

    def __init__(self) -> None:
        self._client: Client | None = None

    def connect(self) -> None:
        try:
            self._client = create_client(
                settings.supabase_url,
                settings.supabase_service_role_key,
            )
            log.info("Supabase client initialized — %s", settings.supabase_url)
        except Exception as exc:
            log.error("Supabase init failed: %s", exc)
            self._client = None

    async def ping(self) -> bool:
        if not self._client:
            return False
        try:
            await asyncio.to_thread(
                lambda: self._client.table("caffeine_items").select("id").limit(1).execute()  # type: ignore[union-attr]
            )
            return True
        except Exception:
            return False

    @property
    def client(self) -> Client | None:
        return self._client


# ─────────────────────────────────────────────────────────────────────────────
# 9. APPLICATION SINGLETONS
# ─────────────────────────────────────────────────────────────────────────────

supabase_mgr  = SupabaseManager()
focus_mgr     = FocusStateManager()
normalizer    = TextNormalizer()
pipeline_mgr  = IngestionPipeline()
scraper       = USCCBScraper(normalizer)

supabase_client: Client | None = None
CAFFEINE_REF_DB: dict[str, dict[str, Any]] = {}

try:
    _caffeine_json = json.loads((BASE_DIR / "caffeine_db.json").read_text())
    for _item in _caffeine_json.get("items", []):
        CAFFEINE_REF_DB[_item["name"].lower()] = _item
    log.info("Loaded %d caffeine reference items into local lookup", len(CAFFEINE_REF_DB))
except Exception as e:
    log.warning("Could not build local caffeine lookup mapping reference: %s", e)


def apply_in_context_rules(text: str, rules: dict[str, Any]) -> tuple[str, list[str]]:
    """Applies dynamic in-context RegEx mapping corrections to input text.
    Malformed regex patterns are skipped with a warning rather than crashing the request.
    """
    tracking_logs = []
    normalized = text
    if isinstance(rules, dict):
        for pattern, replacement in rules.items():
            try:
                if re.search(pattern, normalized, re.IGNORECASE):
                    normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)
                    tracking_logs.append(f"RegEx Match: '{pattern}' replaced with '{replacement}'")
            except re.error as regex_err:
                log.warning("Skipping malformed correction rule pattern '%s': %s", pattern, regex_err)
    return normalized, tracking_logs


def _strip_llm_fences(raw: str) -> str:
    """Remove markdown code fences from LLM output before JSON parsing.
    The original strip('`') approach was corrupting JSON values containing
    the substring 'json' and stripping chars from both ends rather than
    removing the fence block as a whole.
    """
    cleaned = re.sub(r'^```(?:json)?\s*', '', raw.strip())
    cleaned = re.sub(r'\s*```$', '', cleaned.strip())
    return cleaned.strip()


def _caffeine_fuzzy_lookup(search_key: str) -> dict[str, Any]:
    """Exact match first, then partial substring match, then default fallback.
    Prevents None return when Ollama extracts a key slightly different from
    what is stored in the caffeine DB (e.g. 'red bull sugar free' vs 'sugar free red bull').
    """
    key = search_key.lower().strip()
    if key in CAFFEINE_REF_DB:
        return CAFFEINE_REF_DB[key]
    # Partial match: find first DB key that is a substring of the search key or vice versa
    for db_key, db_item in CAFFEINE_REF_DB.items():
        if db_key in key or key in db_key:
            log.info("Caffeine fuzzy match: '%s' -> '%s'", key, db_key)
            return db_item
    log.warning("No caffeine match for '%s', using 80mg default", key)
    return {"name": search_key or "Unknown Drink", "caffeine_mg": 80}


async def verify_user_token(authorization: str = Header(None)) -> str:
    """Extracts the Bearer token and validates it against Supabase auth."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="Missing or malformed Authorization header."
        )
    if not supabase_client:
        raise HTTPException(status_code=503, detail="Supabase service unavailable.")

    try:
        token = authorization.split(" ")[1]
        user_info = supabase_client.auth.get_user(token)
        return str(user_info.user.id)
    except Exception as e:
        raise HTTPException(
            status_code=401,
            detail=f"Security token validation failed: {str(e)}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# 10. LIFESPAN
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Voice Pipeline Backend starting up...")

    supabase_mgr.connect()
    client = supabase_mgr.client

    global supabase_client
    supabase_client = client

    focus_mgr.supabase    = client
    pipeline_mgr.supabase = client
    scraper.supabase      = client

    if client:
        run = await pipeline_mgr.run_caffeine_db()
        log.info("Seeding caffeine_db pipeline queued — run id: %s", run.id)

    log.info("Startup complete. Focus state: %s", focus_mgr.current.state)
    yield

    log.info("Shutting down...")
    focus_mgr.reset()


# ─────────────────────────────────────────────────────────────────────────────
# 11. FASTAPI APP
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Voice Pipeline System",
    version="1.1.0",
    description="Production backend: ingestion pipelines, focus states, text normalization, USCCB readings",
    lifespan=lifespan,
)

# FIX: allow_origins=["*"] with allow_credentials=True is rejected by all modern browsers.
# Use explicit origin list from settings. Add your Tailscale IP to CORS_ORIGINS in .env.
app.add_middleware(
    CORSMiddleware,
    allow_origins=_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────────────────────
# 12. ROUTES — Root & Health
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/", tags=["System"])
async def root() -> dict[str, str]:
    """Root route — silences 404 log noise from browser favicon/root requests."""
    return {"status": "Voice Pipeline System online", "docs": "/docs"}


@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check() -> HealthResponse:
    connected = await supabase_mgr.ping()
    return HealthResponse(
        status="ok",
        version="1.1.0",
        focus_state=focus_mgr.current.state,
        supabase_connected=connected,
        active_pipelines=pipeline_mgr.active_count(),
        timestamp=dt_class.now(UTC),
    )


# ─────────────────────────────────────────────────────────────────────────────
# 13. ROUTES — Focus State
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/focus", response_model=FocusStateResponse, tags=["Focus State"])
async def get_focus_state() -> FocusStateResponse:
    return focus_mgr.current


@app.post("/focus/{target_state}", response_model=FocusStateResponse, tags=["Focus State"])
async def set_focus_state(
    target_state: FocusState,
    context: dict[str, Any] | None = None,
) -> FocusStateResponse:
    try:
        return focus_mgr.transition(target_state, context=context)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))


@app.get("/focus/history", response_model=list[FocusStateResponse], tags=["Focus State"])
async def focus_history(limit: int = 20) -> list[FocusStateResponse]:
    return focus_mgr.history(limit)


@app.get("/focus/transitions", response_model=list[FocusState], tags=["Focus State"])
async def allowed_transitions() -> list[FocusState]:
    return focus_mgr.allowed_transitions()


# ─────────────────────────────────────────────────────────────────────────────
# 14. ROUTES — Text Normalization
# ─────────────────────────────────────────────────────────────────────────────

class NormalizeRequest(BaseModel):
    text: str = Field(..., min_length=1, max_length=10_000)


@app.post("/normalize", response_model=NormalizationResult, tags=["Text Normalization"])
async def normalize_text(body: NormalizeRequest) -> NormalizationResult:
    return normalizer.normalize(body.text)


@app.post("/normalize/batch", response_model=list[NormalizationResult], tags=["Text Normalization"])
async def normalize_batch(texts: list[NormalizeRequest]) -> list[NormalizationResult]:
    if len(texts) > 50:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Batch limited to 50 items",
        )
    return [normalizer.normalize(t.text) for t in texts]


# ─────────────────────────────────────────────────────────────────────────────
# 15. ROUTES — USCCB Readings
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/readings", response_model=DailyReading, tags=["USCCB Readings"])
async def get_todays_readings() -> DailyReading:
    return await scraper.get_readings()


@app.get("/readings/{reading_date}", response_model=DailyReading, tags=["USCCB Readings"])
async def get_readings_by_date(reading_date: date) -> DailyReading:
    if reading_date > date.today() + timedelta(days=1):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot pre-fetch readings more than 1 day in advance",
        )
    return await scraper.get_readings(reading_date)


@app.delete("/readings/cache/{reading_date}", tags=["USCCB Readings"])
async def invalidate_cache(reading_date: date) -> dict[str, Any]:
    evicted = scraper.invalidate(reading_date)
    return {"date": reading_date.isoformat(), "evicted": evicted}


@app.get("/readings/cache/stats", tags=["USCCB Readings"])
async def cache_stats() -> dict[str, Any]:
    return scraper.cache_stats()


# ─────────────────────────────────────────────────────────────────────────────
# 16. ROUTES — Ingestion Pipelines
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/pipelines/caffeine-db", response_model=PipelineRun, tags=["Pipelines"])
async def trigger_caffeine_db_pipeline() -> PipelineRun:
    return await pipeline_mgr.run_caffeine_db()


@app.post("/pipelines/usccb", response_model=PipelineRun, tags=["Pipelines"])
async def trigger_usccb_pipeline(target_date: date | None = None) -> PipelineRun:
    return await pipeline_mgr.run_usccb(scraper, target_date)


@app.get("/pipelines", response_model=list[PipelineRun], tags=["Pipelines"])
async def list_pipeline_runs() -> list[PipelineRun]:
    return pipeline_mgr.list_runs()


@app.get("/pipelines/{run_id}", response_model=PipelineRun, tags=["Pipelines"])
async def get_pipeline_run(run_id: str) -> PipelineRun:
    run = pipeline_mgr.get_run(run_id)
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    return run


# ─────────────────────────────────────────────────────────────────────────────
# 17. ROUTES — Caffeine DB
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/caffeine", response_model=list[CaffeineItem], tags=["Caffeine DB"])
async def list_caffeine_items(
    category: str | None = None,
    sugar_free: bool | None = None,
    max_mg: int | None = None,
    min_mg: int | None = None,
) -> list[CaffeineItem]:
    raw: list[dict] = json.loads((BASE_DIR / "caffeine_db.json").read_text())["items"]
    items = [CaffeineItem(**i) for i in raw]

    if category is not None:
        items = [i for i in items if i.category == category]
    if sugar_free is not None:
        items = [i for i in items if i.sugar_free == sugar_free]
    if max_mg is not None:
        items = [i for i in items if i.caffeine_mg <= max_mg]
    if min_mg is not None:
        items = [i for i in items if i.caffeine_mg >= min_mg]

    return items


@app.get("/caffeine/{item_id}", response_model=CaffeineItem, tags=["Caffeine DB"])
async def get_caffeine_item(item_id: int) -> CaffeineItem:
    raw: list[dict] = json.loads((BASE_DIR / "caffeine_db.json").read_text())["items"]
    for item in raw:
        if item["id"] == item_id:
            return CaffeineItem(**item)
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")


# ─────────────────────────────────────────────────────────────────────────────
# 18. ROUTES — Voice Parser Engine
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/parse", response_model=IngestionIntentResponse, tags=["Voice Parser Engine"])
async def process_voice_ingestion(
    payload: UtterancePayload,
    user_id: str = Depends(verify_user_token),
) -> IngestionIntentResponse:
    if not supabase_client:
        raise HTTPException(status_code=503, detail="Supabase runtime integration client missing.")

    # ── Idempotency check ─────────────────────────────────────────────────────
    # If this UUID was already processed, return the stored result immediately
    # without re-running inference. Protects against duplicate submissions over
    # intermittent Tailscale connections and iOS retry storms.
    existing = (
        supabase_client.table("tasks")
        .select("id, title, category")
        .eq("idempotency_key", payload.id)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    )
    if existing.data:
        log.info("Idempotency hit for key %s — returning cached response", payload.id)
        return IngestionIntentResponse(
            intent="log_task",
            structured_data=existing.data[0],
            applied_corrections=["idempotency_cache_hit"],
        )

    # ── Fetch user rules and focus context ────────────────────────────────────
    pref_res = (
        supabase_client.table("user_preferences")
        .select("correction_rules")
        .eq("user_id", user_id)
        .execute()
    )
    focus_res = (
        supabase_client.table("focus_states")
        .select("active_mode")
        .eq("user_id", user_id)
        .execute()
    )

    rules = pref_res.data[0].get("correction_rules", {}) if pref_res.data else {}
    focus_mode = focus_res.data[0].get("active_mode", "standard") if focus_res.data else "standard"

    # ── Apply in-context normalization rules ──────────────────────────────────
    normalized_text, tracking_logs = apply_in_context_rules(payload.raw_transcript, rules)

    # ── Pull recent corrections for few-shot context ──────────────────────────
    corrections_res = (
        supabase_client.table("corrections")
        .select("original_transcript", "original_output", "corrected_output")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .limit(10)
        .execute()
    )

    few_shot_context = ""
    if corrections_res.data:
        few_shot_context = "\n\nPreviously the user corrected these outputs manually:\n"
        for entry in corrections_res.data:
            few_shot_context += (
                f"Input: \"{entry['original_transcript']}\"\n"
                f"Wrong output: {json.dumps(entry['original_output'])}\n"
                f"Correct output: {json.dumps(entry['corrected_output'])}\n\n"
            )

    # ── Build system prompt ───────────────────────────────────────────────────
    system_prompt = f"""You are a precise, deterministic voice-to-JSON engine parsing logs for a user currently in '{focus_mode}' focus mode.
Analyze the user transcript and output exactly one structured JSON block matching these explicit schemas.
CRITICAL: Return ONLY raw JSON. No markdown code fences, no backticks, no commentary before or after the JSON.

Intent Matrix Schemas:
1. Intent: "log_task" -> For adding things to do.
   Required fields: {{"title": "string", "due_date": "ISO8601 or null (only set if explicitly stated)", "category": "one of: home/work/investigation/personal/health/faith"}}
2. Intent: "append_case" -> For logging notes/updates to an ongoing case file or investigation.
   Required fields: {{"case_title": "string", "note_entry": "string"}}
3. Intent: "log_caffeine" -> For logging drinks or caffeine intake.
   Required fields: {{"item_search_key": "lowercase drink name", "quantity": 1.0}}
4. Intent: "log_medication" -> For logging medicine doses.
   Required fields: {{"medication_name": "string", "dose_count": 1.0}}{few_shot_context}

Output shape: {{"intent": "one_of_the_above", "data": {{ ... }} }}"""

    # ── Ollama inference ──────────────────────────────────────────────────────
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{OLLAMA_HOST}/api/generate",
                json={
                    "model": "qwen2.5:1.5b",
                    "prompt": (
                        f"System Context:\n{system_prompt}\n\n"
                        f"User Input: \"{normalized_text}\"\n"
                        f"JSON Output:"
                    ),
                    "stream": False,
                    "options": {"temperature": 0.0},
                },
                # FIX: raised from 30s to 60s — Broadwell CPU inference can exceed
                # 30s under load, causing premature 500 errors on valid requests.
                timeout=60.0,
            )
            raw_text = response.json().get("response", "").strip()

            # FIX: replaced strip("`").replace("json","",1) with regex fence removal.
            # The original approach corrupted JSON values containing the substring "json"
            # and stripped characters from both ends of the string rather than the fence block.
            cleaned_text = _strip_llm_fences(raw_text)

            try:
                parsed_output = json.loads(cleaned_text)
            except json.JSONDecodeError as json_err:
                # Expose the actual malformed output in the error body for debugging.
                raise HTTPException(
                    status_code=500,
                    detail=(
                        f"LLM returned non-parseable JSON. "
                        f"JSONDecodeError: {json_err}. "
                        f"Cleaned output (first 400 chars): {cleaned_text[:400]}"
                    ),
                )

        except HTTPException:
            # FIX: re-raise HTTPExceptions before the broad except catches them.
            # Without this, intentional 4xx/5xx responses were being swallowed
            # and replaced with a generic 500.
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Local LLM runtime exception: {type(e).__name__}: {str(e)}",
            )

    intent = parsed_output.get("intent", "unknown")
    inner_data = parsed_output.get("data", {})

    # ── Route to Supabase tables ──────────────────────────────────────────────
    if intent == "log_task":
        supabase_client.table("tasks").insert({
            "user_id": user_id,
            "idempotency_key": payload.id,
            "title": inner_data.get("title", normalized_text),
            "due_date": inner_data.get("due_date"),
            "category": inner_data.get("category", "personal"),
            "source": payload.source.value,
        }).execute()

    elif intent == "append_case":
        case_title = inner_data.get("case_title", "General Notes")
        note_entry = inner_data.get("note_entry", normalized_text)
        existing_case = (
            supabase_client.table("cases")
            .select("id, notes")
            .eq("user_id", user_id)
            .eq("title", case_title)
            .execute()
        )
        if existing_case.data:
            case_id = existing_case.data[0]["id"]
            # FIX: guard against None notes field with `or []` to prevent
            # NoneType.append() crash on cases that were created with null notes.
            current_notes = existing_case.data[0].get("notes") or []
            current_notes.append({
                "timestamp": dt_class.now(UTC).isoformat(),
                "entry": note_entry,
            })
            supabase_client.table("cases").update(
                {"notes": current_notes}
            ).eq("id", case_id).execute()
        else:
            supabase_client.table("cases").insert({
                "user_id": user_id,
                "title": case_title,
                "notes": [{"timestamp": dt_class.now(UTC).isoformat(), "entry": note_entry}],
            }).execute()

    elif intent == "log_caffeine":
        search_key = inner_data.get("item_search_key", "")
        # FIX: replaced direct dict.get() with fuzzy lookup that handles partial
        # key matches. Prevents silent 80mg default on every non-exact match.
        matched_item = _caffeine_fuzzy_lookup(search_key)
        total_mg = float(matched_item["caffeine_mg"]) * float(inner_data.get("quantity", 1.0))
        supabase_client.table("health_logs").insert({
            "user_id": user_id,
            "metric_type": "caffeine",
            "item_name": matched_item["name"],
            "value": total_mg,
        }).execute()
        inner_data["calculated_total_mg"] = total_mg

    elif intent == "log_medication":
        supabase_client.table("health_logs").insert({
            "user_id": user_id,
            "metric_type": "medication",
            "item_name": inner_data.get("medication_name", "Unknown Medicine"),
            "value": inner_data.get("dose_count", 1.0),
        }).execute()

    else:
        # FIX: replaced HTTPException 422 with graceful log + passthrough.
        # Raising 422 on unrecognized intent caused the iOS client to mark
        # valid utterances as permanently failed in the sync queue.
        log.warning(
            "Unrouted intent '%s' for user %s — transcript returned without persistence: %s",
            intent, user_id, normalized_text[:100],
        )
        tracking_logs.append(
            f"Unrouted intent '{intent}': no table write performed. "
            f"Submit a correction via POST /correct to improve future routing."
        )

    return IngestionIntentResponse(
        intent=intent,
        structured_data=inner_data,
        applied_corrections=tracking_logs,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 19. ROUTES — Correction Engine
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/correct", tags=["Correction Engine"])
async def register_pipeline_correction(
    payload: CorrectionSubmission,
    user_id: str = Depends(verify_user_token),
) -> dict[str, str]:
    if not supabase_client:
        raise HTTPException(status_code=503, detail="Supabase runtime database client missing.")
    try:
        supabase_client.table("corrections").insert({
            "user_id": user_id,
            "original_transcript": payload.original_transcript,
            "original_output": payload.original_output,
            "corrected_output": payload.corrected_output,
        }).execute()
        return {"status": "success", "message": "Correction logged to dynamic engine."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to commit correction log: {str(e)}")


# ─────────────────────────────────────────────────────────────────────────────
# 19b. STATIC FILES — catch-all mount MUST be last, after all API routes
# ─────────────────────────────────────────────────────────────────────────────

app.mount("/", StaticFiles(directory="static", html=True), name="static")


# 20. ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level=settings.log_level,
    )
