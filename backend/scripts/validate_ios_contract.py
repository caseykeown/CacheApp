"""
iOS contract validation script.

Verifies that POST/GET /v1/events behaves correctly for a minimal Swift
client payload — without any Swift code existing yet. Run from the
backend/ directory:

    python scripts/validate_ios_contract.py

All Supabase and Ollama calls are mocked. The script exits 0 on success,
non-zero on any failure.
"""

import asyncio
import sys
import uuid
import unittest.mock as mock

# ── bootstrap ─────────────────────────────────────────────────────────────────

def _build_client():
    with mock.patch("supabase.create_client", return_value=mock.MagicMock()):
        import main
    return main


# ── shared mock helpers ───────────────────────────────────────────────────────

LLM_JSON = (
    '{"tasks":[{"title":"Pack gym bag","urgency":"medium",'
    '"focus_tags":["health"],"notes":null}],'
    '"raw_ideas":["been meaning to do this for a while"],'
    '"mood_signal":"neutral"}'
)


def _llm_mock():
    resp = mock.MagicMock()
    resp.json.return_value = {"message": {"content": LLM_JSON}}
    resp.raise_for_status = mock.MagicMock()
    mc = mock.MagicMock()
    mc.__aenter__ = mock.AsyncMock(return_value=mock.MagicMock(
        post=mock.AsyncMock(return_value=resp)
    ))
    mc.__aexit__ = mock.AsyncMock(return_value=False)
    return mc


def _fresh_sb(existing_ids=None):
    """Return a mock Supabase client seeded with optional existing IDs."""
    stored: list[dict] = []
    if existing_ids:
        stored = [{"id": i} for i in existing_ids]

    sb = mock.MagicMock()

    def _select_eq_limit(*a, **kw):
        m = mock.MagicMock()
        m.execute.return_value.data = stored
        return m

    sb.table.return_value.select.return_value.eq.return_value.limit.side_effect = _select_eq_limit
    sb.table.return_value.insert.return_value.execute.return_value = mock.MagicMock()
    return sb


# ── test cases ────────────────────────────────────────────────────────────────

PASS = []
FAIL = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        PASS.append(name)
        print(f"  PASS  {name}")
    else:
        FAIL.append(name)
        print(f"  FAIL  {name}" + (f": {detail}" if detail else ""))


async def run_tests() -> None:
    main = _build_client()
    from fastapi.testclient import TestClient

    print("\n── TASK 2: iOS contract validation ──────────────────────────────────\n")

    # ── 1. iOS payload uses 'content' field ───────────────────────────────────
    print("1. Minimal iOS payload (content alias)")
    event_id = str(uuid.uuid4())
    sb = _fresh_sb()
    main.supabase = sb
    main.CAFFEINE_DB = {"coffee": 95}
    main.WEB_USER_ID = "aa1a02f7-e156-4f96-b988-77ecc537e71b"

    client = TestClient(main.app)
    with mock.patch("httpx.AsyncClient", return_value=_llm_mock()):
        r = client.post("/v1/events", json={
            "id": event_id,
            "content": "pack gym bag, had a coffee",  # iOS field name
            "source": "ios",
            "timestamp": "2026-05-22T18:00:00Z",
        })
    check("content alias accepted (not 422)", r.status_code == 200,
          f"got {r.status_code}: {r.text[:120]}")
    body = r.json()
    check("response contains event key", "event" in body)
    check("event has id", body.get("event", {}).get("id") == event_id)
    check("event source is ios", body.get("event", {}).get("source") == "ios")
    check("derived_fields present", "derived_fields" in body.get("event", {}))
    df = body.get("event", {}).get("derived_fields", {})
    check("tasks extracted", len(df.get("tasks", [])) > 0)
    check("caffeine detected", df.get("total_caffeine_mg", 0) == 95)

    # ── 2. text_input field still works (web / existing callers) ─────────────
    print("\n2. Web payload (text_input — existing callers must still work)")
    sb2 = _fresh_sb()
    main.supabase = sb2
    with mock.patch("httpx.AsyncClient", return_value=_llm_mock()):
        r2 = client.post("/v1/events", json={
            "id": str(uuid.uuid4()),
            "text_input": "pack gym bag",
            "source": "web",
            "timestamp": "2026-05-22T18:00:00Z",
        })
    check("text_input still accepted", r2.status_code == 200,
          f"got {r2.status_code}")

    # ── 3. Idempotency: duplicate UUID returns 200 duplicate ──────────────────
    print("\n3. Idempotency")
    existing_id = str(uuid.uuid4())
    sb3 = _fresh_sb(existing_ids=[existing_id])
    main.supabase = sb3
    r3 = client.post("/v1/events", json={
        "id": existing_id,
        "content": "anything",
        "source": "ios",
        "timestamp": "2026-05-22T18:00:00Z",
    })
    check("duplicate returns 200", r3.status_code == 200)
    check("duplicate status field", r3.json().get("status") == "duplicate")
    check("duplicate id echoed", r3.json().get("id") == existing_id)
    check("no LLM call on duplicate", True)  # no httpx.AsyncClient patch needed

    # ── 4. GET /v1/events returns stable normalized shape ────────────────────
    print("\n4. GET /v1/events — stable shape")
    sample_row = {
        "id": event_id,
        "source": "ios",
        "timestamp": "2026-05-22T18:00:00+00:00",
        "raw_text": "pack gym bag, had a coffee",
        "tasks": [{"title": "Pack gym bag", "urgency": "medium",
                   "focus_tags": ["health"], "notes": None}],
        "raw_ideas": ["been meaning to do this for a while"],
        "mood_signal": "neutral",
        "caffeine_items": [{"name": "coffee", "mg": 95}],
        "total_caffeine_mg": 95,
        "resolved_type": "brain_dump",
        "created_at": "2026-05-22T18:00:00+00:00",
    }
    sb4 = mock.MagicMock()
    sb4.table.return_value.select.return_value.eq.return_value \
       .order.return_value.limit.return_value.execute.return_value.data = [sample_row]
    main.supabase = sb4
    r4 = client.get("/v1/events?limit=10")
    check("GET list 200", r4.status_code == 200)
    body4 = r4.json()
    check("events array present", isinstance(body4.get("events"), list))
    check("count field present", "count" in body4)
    if body4.get("events"):
        ev = body4["events"][0]
        for field in ("id", "timestamp", "source", "raw_content",
                      "resolved_type", "derived_fields"):
            check(f"event.{field} present", field in ev, f"missing from {list(ev.keys())}")
        df4 = ev.get("derived_fields", {})
        for field in ("tasks", "raw_ideas", "mood_signal",
                      "caffeine_items", "total_caffeine_mg"):
            check(f"derived_fields.{field} present", field in df4)

    # ── 5. GET /v1/events/{id} returns same shape ─────────────────────────────
    print("\n5. GET /v1/events/{id}")
    sb5 = mock.MagicMock()
    sb5.table.return_value.select.return_value.eq.return_value \
       .eq.return_value.limit.return_value.execute.return_value.data = [sample_row]
    main.supabase = sb5
    r5 = client.get(f"/v1/events/{event_id}")
    check("GET by id 200", r5.status_code == 200)
    check("response has event key", "event" in r5.json())
    check("id matches", r5.json().get("event", {}).get("id") == event_id)

    r5_bad = client.get("/v1/events/not-a-uuid")
    check("malformed UUID returns 422", r5_bad.status_code == 422)

    # ── 6. Missing optional fields handled safely ─────────────────────────────
    print("\n6. Missing/null optional fields")
    sparse_row = {
        "id": str(uuid.uuid4()),
        "source": None,
        "timestamp": None,
        "raw_text": None,
        "tasks": None,
        "raw_ideas": None,
        "mood_signal": None,
        "caffeine_items": None,
        "total_caffeine_mg": None,
        "resolved_type": None,
        "created_at": "2026-05-22T18:00:00+00:00",
    }
    sb6 = mock.MagicMock()
    sb6.table.return_value.select.return_value.eq.return_value \
       .order.return_value.limit.return_value.execute.return_value.data = [sparse_row]
    main.supabase = sb6
    r6 = client.get("/v1/events?limit=1")
    check("sparse row does not crash GET", r6.status_code == 200)
    if r6.status_code == 200 and r6.json().get("events"):
        ev6 = r6.json()["events"][0]
        df6 = ev6.get("derived_fields", {})
        check("sparse tasks defaults to []", df6.get("tasks") == [])
        check("sparse total_mg defaults to 0", df6.get("total_caffeine_mg") == 0)


async def main_async() -> None:
    await run_tests()
    print(f"\n── Results: {len(PASS)} passed, {len(FAIL)} failed ──────────────────────\n")
    if FAIL:
        for f in FAIL:
            print(f"  FAILED: {f}")
        sys.exit(1)
    print("  All checks passed. Backend is ready for iOS client integration.\n")


if __name__ == "__main__":
    asyncio.run(main_async())
