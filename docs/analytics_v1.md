# Analytics v1 (Issue #9)

This layer provides query-ready analytics over pipeline data.

## Refresh

```bash
python pipeline/jobs/run_analytics_refresh.py
```

This does:
- applies `sql/analytics_v1.sql`
- refreshes dimensions
- writes daily quality row in `analytics.quality_daily`

## Main objects

- `analytics.quality_daily`
- `analytics.dim_institutions`
- `analytics.dim_companies`
- `analytics.fact_calls`
- `analytics.fact_outcomes`
- `analytics.fact_specs`
- `analytics.v_call_aggregates_by_filters`
- `analytics.v_institution_company_relationships`
- `analytics.v_company_dependency`

## Query cookbook

Use:

```sql
\i sql/query_cookbook_v1.sql
```

Or copy specific queries from that file into your SQL client.

## Notes

- Relationship/dependency views improve as winner/runner-up extraction improves.
- `publish_at` completeness is still an active quality stream.
