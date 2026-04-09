# RUPE Pipeline

Pipeline to ingest, normalize, score, and assign procurement opportunities from Uruguay public sources (ARCE/Compras Estatales + RUPE datasets).

## MVP modules

- `pipeline/collectors/` — fetch listings, details, and RUPE datasets
- `pipeline/transforms/` — normalize, resolve entities, score, assign
- `pipeline/delivery/` — digest/alert outputs
- `pipeline/sql/` — schema and migrations
- `pipeline/config/businesses/` — matching profiles per business

## Quickstart

```bash
# recommended helper
./scripts/bootstrap_local.sh

# or manual
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Run a sample collector:

```bash
python pipeline/collectors/compras_listings.py
```

Run demo scoring + digest:

```bash
python pipeline/main.py
```

Run listings ETL (parse + normalize + DB upsert):

```bash
# requires DATABASE_URL or local postgres in defaults
python pipeline/jobs/run_listings_etl.py
```

Run RSS ETL (recommended incremental source):

```bash
python pipeline/jobs/run_rss_etl.py
```

Run RUPE supplier ingestion:

```bash
python pipeline/jobs/run_rupe_ingest.py
```

Run entity resolution + assignment and digest generation:

```bash
python pipeline/jobs/run_entity_resolution.py
python pipeline/jobs/run_assignment_etl.py
python pipeline/jobs/run_digest.py
```

Run detail enrichment (fills richer descriptions/buyer hints):

```bash
python pipeline/jobs/run_details_enrich.py
```

Run full sequence:

```bash
python pipeline/jobs/run_all.py
```

Raw snapshot + replay utilities:

```bash
python pipeline/jobs/run_capture_raw_details.py
python pipeline/jobs/run_replay_from_raw.py
```

Attachment ingestion (download + extract + summary):

```bash
python pipeline/jobs/run_attachment_ingest.py
python pipeline/jobs/run_archive_extract.py   # unzip/7z extract + child file processing
python pipeline/jobs/run_specs_extract.py     # extract searchable item/product/service specs
```

Open-call refresh (status + deadline urgency view):

```bash
python pipeline/jobs/run_open_calls_refresh.py
# query view: v_open_calls_with_deadlines
```

Analytics layer refresh (Issue #9):

```bash
python pipeline/jobs/run_analytics_refresh.py
# applies sql/analytics_v1.sql and updates analytics.quality_daily + dimensions
```

Query cookbook:

```bash
# see ready-to-run examples
cat sql/query_cookbook_v1.sql
# docs
cat docs/analytics_v1.md
```

Closed-call outcomes (winner + runner-up when present):

```bash
python pipeline/jobs/run_outcomes_extract.py
```

Sync local raw files to Supabase Storage bucket:

```bash
python pipeline/jobs/run_sync_storage.py
```
(uses `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_STORAGE_BUCKET`)

## Suggested cron schedule

```cron
*/30 * * * *  cd /path/to/rupe && . .venv/bin/activate && python pipeline/jobs/run_rss_etl.py
35 * * * *    cd /path/to/rupe && . .venv/bin/activate && python pipeline/jobs/run_details_enrich.py
40 * * * *    cd /path/to/rupe && . .venv/bin/activate && python pipeline/jobs/run_entity_resolution.py
5 * * * *     cd /path/to/rupe && . .venv/bin/activate && python pipeline/jobs/run_assignment_etl.py
10 8 * * *    cd /path/to/rupe && . .venv/bin/activate && python pipeline/jobs/run_digest.py
```

If cron is unavailable on host/container, run background loop:

```bash
nohup ./scripts/start_fetch_loop.sh 30 > logs/nohup-loop.out 2>&1 &
```
