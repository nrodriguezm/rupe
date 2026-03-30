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
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run a sample collector:

```bash
python pipeline/collectors/compras_listings.py
```
