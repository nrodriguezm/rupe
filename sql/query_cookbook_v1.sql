-- Query Cookbook v1 (Issue #9)
-- Run after: python pipeline/jobs/run_analytics_refresh.py

-- 1) Top institutions by call volume
select institution, sum(calls) as calls, sum(total_amount) as total_amount
from analytics.v_call_aggregates_by_filters
where institution <> 'unknown'
group by 1
order by calls desc
limit 20;

-- 2) Top institutions by estimated total amount
select institution, sum(total_amount) as total_amount, sum(calls) as calls
from analytics.v_call_aggregates_by_filters
where institution <> 'unknown'
group by 1
order by total_amount desc nulls last
limit 20;

-- 3) Monthly trend by category
select month, category, sum(calls) as calls, sum(total_amount) as total_amount
from analytics.v_call_aggregates_by_filters
group by 1,2
order by month, calls desc;

-- 4) Open vs closed by institution
select institution,
       sum(open_calls) as open_calls,
       sum(closed_calls) as closed_calls,
       sum(calls) as total_calls
from analytics.v_call_aggregates_by_filters
where institution <> 'unknown'
group by 1
order by total_calls desc
limit 30;

-- 5) Company dependency by wins (top pairs)
select company, institution, wins, wins_dependency_pct, amount_dependency_pct
from analytics.v_company_dependency
where company <> 'unknown' and institution <> 'unknown'
order by wins desc
limit 50;

-- 6) Relationship intensity (institution ↔ company)
select institution, company, wins, amount_sum, first_seen, last_seen
from analytics.v_institution_company_relationships
where institution <> 'unknown' and company <> 'unknown'
order by wins desc
limit 50;

-- 7) Calls in date range with filters (example)
select opportunity_id, external_id, title, buyer_name, category, amount, currency, publish_at, deadline_at, source_url
from analytics.fact_calls
where publish_at >= '2025-01-01' and publish_at < '2026-01-01'
  and status = 'open'
  and (category ilike '%Licitación%' or category ilike '%Concurso%')
order by publish_at desc
limit 200;

-- 8) Spec search (products/services)
select s.external_id, s.spec_type, s.confidence, left(s.spec_text, 240) as spec, o.source_url
from analytics.fact_specs s
join analytics.fact_calls o on o.opportunity_id = s.opportunity_id
where s.spec_text ilike '%concentrador%oxígeno%'
order by s.confidence desc, s.spec_id desc
limit 100;

-- 9) Attachment extraction quality snapshot
select
  count(*) as attachments_total,
  count(*) filter (where extracted_text is not null and length(extracted_text)>0) as with_text,
  count(*) filter (where summary is not null and length(summary)>0) as with_summary,
  count(*) filter (where extraction_status like 'archive%') as archives
from opportunity_attachments;

-- 10) Data quality daily trend
select day,
       opportunities_total,
       publish_at_filled,
       category_filled,
       amount_filled,
       outcomes_filled,
       attachments_total,
       attachments_with_text,
       specs_total
from analytics.quality_daily
order by day desc
limit 30;
