-- Analytics v1 schema/views for Issue #9

create schema if not exists analytics;

-- 1) Daily data quality scorecard
create table if not exists analytics.quality_daily (
  day date primary key,
  opportunities_total bigint,
  publish_at_filled bigint,
  category_filled bigint,
  amount_filled bigint,
  outcomes_filled bigint,
  attachments_total bigint,
  attachments_with_text bigint,
  specs_total bigint,
  buyers_linked bigint,
  companies_linked bigint,
  created_at timestamptz default now()
);

-- 2) Canonical dimensions
create table if not exists analytics.dim_institutions (
  institution_id bigserial primary key,
  institution_name text unique not null,
  normalized_name text,
  confidence numeric,
  created_at timestamptz default now()
);

create table if not exists analytics.dim_companies (
  company_id bigserial primary key,
  rupe_identification text,
  legal_name text not null,
  normalized_name text,
  source_period text,
  unique (rupe_identification, legal_name)
);

-- 3) Fact views
create or replace view analytics.fact_calls as
select
  o.id as opportunity_id,
  o.external_id,
  o.title,
  o.description,
  o.buyer_name,
  o.publish_at,
  o.deadline_at,
  o.status,
  o.amount,
  o.currency,
  o.category,
  o.department,
  o.source_url,
  o.buyer_rupe_identification,
  o.buyer_rupe_name,
  o.buyer_match_score
from opportunities o;

create or replace view analytics.fact_outcomes as
select
  oo.id as outcome_id,
  oo.opportunity_id,
  oo.external_id,
  oo.winner_name,
  oo.runner_up_name,
  oo.confidence,
  oo.parsed_at
from opportunity_outcomes oo;

create or replace view analytics.fact_specs as
select
  s.id as spec_id,
  s.opportunity_id,
  s.attachment_id,
  s.external_id,
  s.spec_text,
  s.spec_type,
  s.confidence,
  s.created_at
from attachment_specs s;

-- 4) Query views
create or replace view analytics.v_call_aggregates_by_filters as
select
  coalesce(to_char(publish_at, 'YYYY-MM'), 'unknown') as month,
  coalesce(category, 'unknown') as category,
  coalesce(buyer_name, 'unknown') as institution,
  count(*) as calls,
  count(*) filter (where status='open') as open_calls,
  count(*) filter (where status='closed') as closed_calls,
  sum(amount) as total_amount
from opportunities
group by 1,2,3;

create or replace view analytics.v_institution_company_relationships as
select
  coalesce(o.buyer_name, 'unknown') as institution,
  coalesce(oo.winner_name, 'unknown') as company,
  count(*) as wins,
  sum(o.amount) as amount_sum,
  min(o.publish_at) as first_seen,
  max(o.publish_at) as last_seen
from opportunities o
join opportunity_outcomes oo on oo.opportunity_id = o.id
where oo.winner_name is not null
group by 1,2;

create or replace view analytics.v_company_dependency as
with base as (
  select
    coalesce(oo.winner_name, 'unknown') as company,
    coalesce(o.buyer_name, 'unknown') as institution,
    count(*) as wins,
    coalesce(sum(o.amount),0) as amount_sum
  from opportunities o
  join opportunity_outcomes oo on oo.opportunity_id = o.id
  where oo.winner_name is not null
  group by 1,2
), totals as (
  select company, sum(wins) as total_wins, sum(amount_sum) as total_amount
  from base
  group by company
)
select
  b.company,
  b.institution,
  b.wins,
  b.amount_sum,
  t.total_wins,
  t.total_amount,
  case when t.total_wins > 0 then round((b.wins::numeric / t.total_wins)*100,2) else 0 end as wins_dependency_pct,
  case when t.total_amount > 0 then round((b.amount_sum / t.total_amount)*100,2) else 0 end as amount_dependency_pct
from base b
join totals t using (company);

-- Helpful indexes
create index if not exists idx_opps_publish on opportunities(publish_at);
create index if not exists idx_opps_category on opportunities(category);
create index if not exists idx_opps_buyer on opportunities(buyer_name);
create index if not exists idx_outcomes_winner on opportunity_outcomes(winner_name);
create index if not exists idx_specs_type on attachment_specs(spec_type);
