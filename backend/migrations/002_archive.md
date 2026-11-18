# Schema v2 — hot/cold telemetry archiving
`telemetry` gains `updated_at DATETIME(6)` (default `CURRENT_TIMESTAMP(6)` on insert/update); `seed_demo.ensure_schema` backfills it on pre-v2 databases.
New tables:
- `telemetry_archive` (cold): same business columns as `telemetry`; migrated rows keep their hot `id`, archive-native backfills take ids from `AUTO_INCREMENT=1e12` so logical ids never collide; `updated_at` is copied from hot and drives the conditional upsert.
- `archive_state` (singleton id=1): current archived `cutoff`; flipped only inside the verified switch transaction.
- `archive_jobs`: one row per archival run — `cutoff`, `prev_cutoff`, `phase` (`copying`→`verifying`→`cleaning`→`completed`, or `failed`), `checkpoint_id` copy watermark, row counters, `switched_at`/`finished_at`. Phases resume idempotently after interruption; cold-side writes are conditional upserts keyed on `updated_at`, so re-runs never duplicate and confirmed corrections always win.
