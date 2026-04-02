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

create table if not exists suppliers_rupe (
  id bigserial primary key,
  source_period text not null,
  country text,
  identification text,
  legal_name text,
  fiscal_address text,
  locality text,
  department text,
  status text,
  raw jsonb not null,
  unique (source_period, identification, legal_name)
);

create table if not exists raw_rss_snapshots (
  id bigserial primary key,
  source_url text not null,
  fetched_at timestamptz default now(),
  payload_xml text,
  payload_path text,
  payload_size_bytes bigint,
  payload_hash text not null,
  item_count int,
  unique (source_url, payload_hash)
);

create table if not exists raw_detail_snapshots (
  id bigserial primary key,
  external_id text not null,
  source_url text not null,
  fetched_at timestamptz default now(),
  payload_html text,
  payload_path text,
  payload_size_bytes bigint,
  payload_hash text not null,
  unique (external_id, payload_hash)
);

alter table raw_rss_snapshots add column if not exists payload_path text;
alter table raw_rss_snapshots add column if not exists payload_size_bytes bigint;
alter table raw_detail_snapshots add column if not exists payload_path text;
alter table raw_detail_snapshots add column if not exists payload_size_bytes bigint;
alter table raw_rss_snapshots alter column payload_xml drop not null;
alter table raw_detail_snapshots alter column payload_html drop not null;
alter table raw_rss_snapshots add column if not exists synced_at timestamptz;
alter table raw_rss_snapshots add column if not exists storage_object_key text;
alter table raw_detail_snapshots add column if not exists synced_at timestamptz;
alter table raw_detail_snapshots add column if not exists storage_object_key text;

create table if not exists opportunity_attachments (
  id bigserial primary key,
  opportunity_id bigint references opportunities(id) on delete cascade,
  external_id text,
  file_url text not null,
  file_name text,
  mime_type text,
  file_size_bytes bigint,
  file_hash text,
  storage_path text,
  storage_object_key text,
  downloaded_at timestamptz,
  extraction_status text,
  extracted_text text,
  summary text,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  unique (external_id, file_url)
);

alter table opportunities add column if not exists parser_version text;
alter table opportunities add column if not exists parsed_at timestamptz;
