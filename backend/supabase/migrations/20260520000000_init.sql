-- Voice Pipeline System — Initial Schema
-- Creates tables consumed by main.py pipelines and state managers.

-- ── caffeine_items ────────────────────────────────────────────────────────────
create table if not exists public.caffeine_items (
    id          integer primary key,
    name        text        not null,
    size_oz     numeric,
    caffeine_mg integer     not null check (caffeine_mg >= 0),
    category    text        not null,
    sugar_free  boolean     not null default false,
    created_at  timestamptz not null default now(),
    updated_at  timestamptz not null default now()
);

create index if not exists idx_caffeine_items_category    on public.caffeine_items (category);
create index if not exists idx_caffeine_items_caffeine_mg on public.caffeine_items (caffeine_mg);

-- ── usccb_readings ────────────────────────────────────────────────────────────
create table if not exists public.usccb_readings (
    id                  bigint generated always as identity primary key,
    reading_date        date        not null unique,
    liturgical_day      text        not null default '',
    readings            jsonb       not null default '[]',
    normalized_readings jsonb       not null default '[]',
    fetched_at          timestamptz not null default now(),
    created_at          timestamptz not null default now()
);

create index if not exists idx_usccb_readings_date on public.usccb_readings (reading_date desc);

-- ── pipeline_runs ─────────────────────────────────────────────────────────────
create table if not exists public.pipeline_runs (
    id                  text        primary key,
    pipeline_type       text        not null,
    status              text        not null,
    started_at          timestamptz not null,
    completed_at        timestamptz,
    records_processed   integer     not null default 0,
    error               text,
    meta                jsonb       not null default '{}'
);

create index if not exists idx_pipeline_runs_type       on public.pipeline_runs (pipeline_type);
create index if not exists idx_pipeline_runs_status     on public.pipeline_runs (status);
create index if not exists idx_pipeline_runs_started_at on public.pipeline_runs (started_at desc);

-- ── focus_state_audit ─────────────────────────────────────────────────────────
create table if not exists public.focus_state_audit (
    id             bigint generated always as identity primary key,
    state          text        not null,
    previous_state text,
    changed_at     timestamptz not null default now(),
    context        jsonb       not null default '{}'
);

create index if not exists idx_focus_state_audit_changed_at on public.focus_state_audit (changed_at desc);
