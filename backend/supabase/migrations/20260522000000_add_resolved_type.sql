-- Adds resolved_type column to tasks table.
-- Closes the gap between the Event model and the database schema.

-- Add column with safe default so existing rows are never null
ALTER TABLE public.tasks
  ADD COLUMN IF NOT EXISTS resolved_type text NOT NULL DEFAULT 'brain_dump';

-- Backfill: rows that look like pure caffeine logs
-- (caffeine items present, no tasks, no raw ideas) → log_caffeine
-- Everything else stays as the default brain_dump
UPDATE public.tasks
SET resolved_type = 'log_caffeine'
WHERE
  caffeine_items IS NOT NULL
  AND jsonb_array_length(caffeine_items) > 0
  AND (tasks IS NULL OR jsonb_array_length(tasks) = 0)
  AND (raw_ideas IS NULL OR jsonb_array_length(raw_ideas) = 0);

-- Index for DB-level filtering via GET /v1/events?type=
CREATE INDEX IF NOT EXISTS idx_tasks_resolved_type
  ON public.tasks (resolved_type);
