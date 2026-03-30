create table if not exists businesses (
  id uuid primary key,
  name text not null,
  rut text,
  active boolean default true,
  created_at timestamptz default now()
);

create table if not exists business_profiles (
  business_id uuid primary key references businesses(id),
  keywords_include text[] default '{}',
  keywords_exclude text[] default '{}',
  categories_include text[] default '{}',
  departments_include text[] default '{}',
  min_amount numeric,
  max_amount numeric,
  urgency_weight numeric default 1.0,
  fit_weight numeric default 1.0,
  updated_at timestamptz default now()
);

create table if not exists opportunities (
  id bigserial primary key,
  source text not null,
  external_id text not null,
  title text not null,
  description text,
  buyer_name text,
  buyer_entity_id uuid,
  publish_at timestamptz,
  deadline_at timestamptz,
  status text,
  amount numeric,
  currency text,
  category text,
  department text,
  source_url text not null,
  raw_hash text not null,
  first_seen_at timestamptz default now(),
  last_seen_at timestamptz default now(),
  unique (source, external_id)
);

create table if not exists opportunity_assignments (
  id bigserial primary key,
  opportunity_id bigint references opportunities(id) on delete cascade,
  business_id uuid references businesses(id),
  score numeric not null,
  reasons jsonb not null,
  assigned_at timestamptz default now(),
  unique (opportunity_id, business_id)
);

create table if not exists pipeline_runs (
  id bigserial primary key,
  job_name text not null,
  started_at timestamptz default now(),
  finished_at timestamptz,
  status text,
  metrics jsonb
);
