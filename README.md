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
