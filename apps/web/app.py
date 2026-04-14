from __future__ import annotations

import os
from typing import Any

import psycopg
import csv
import io

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, PlainTextResponse

app = FastAPI(title="RUPE Analytics UI")


def dsn() -> str:
    return os.getenv("DATABASE_URL", "")


def fetch_all(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    with psycopg.connect(dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    return """
<!doctype html>
<html><head>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>RUPE Analytics</title>
<style>
body{font-family:system-ui;margin:0;background:#0b1020;color:#e8eefc}
.wrap{max-width:1100px;margin:0 auto;padding:16px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}
.card{background:#121a33;border:1px solid #24345f;border-radius:12px;padding:12px}
h1{font-size:20px} table{width:100%;border-collapse:collapse} td,th{border-bottom:1px solid #23345d;padding:8px;font-size:13px}
input,select{background:#0f1730;color:#fff;border:1px solid #2b3d6d;border-radius:8px;padding:6px}
button{background:#4f7cff;color:#fff;border:0;padding:8px 12px;border-radius:8px}
.small{font-size:12px;color:#9fb0d8}
</style></head><body>
<div class='wrap'>
<h1>RUPE Analytics MVP</h1>
<div class='grid' id='kpis'></div>

<div class='card' style='margin-top:12px'>
<h3>Sample Queries</h3>
<div class='small'>Top institutions and company dependency (from analytics views)</div>
<div id='samples'></div>
</div>

<div class='card' style='margin-top:12px'>
<h3>Institution ↔ Company Relationships</h3>
<div class='small'>Top winner relationships to explore dependency patterns</div>
<div id='rels'></div>
</div>

<div class='card' style='margin-top:12px'>
<h3>Filtered Calls</h3>
<div style='display:flex;gap:8px;flex-wrap:wrap'>
<input id='from' placeholder='2025-01-01'>
<input id='to' placeholder='2026-01-01'>
<input id='institution' placeholder='institution contains'>
<select id='status'><option value=''>any status</option><option>open</option><option>closed</option></select>
<input id='category' placeholder='category contains'>
<button onclick='loadCalls()'>Run</button>
<button onclick='downloadCsv()'>Export CSV</button>
</div>
<div id='calls'></div>
</div>
</div>
<script>
async function get(u){const r=await fetch(u);return r.json()}
function table(rows){ if(!rows.length) return '<p class=small>No rows</p>'; const cols=Object.keys(rows[0]);
let h='<table><tr>'+cols.map(c=>`<th>${c}</th>`).join('')+'</tr>';
for(const row of rows){h+='<tr>'+cols.map(c=>`<td>${row[c]??''}</td>`).join('')+'</tr>'}
return h+'</table>' }
async function load(){
 const k=await get('/api/kpis');
 const cards=[['Opportunities',k.opportunities_total],['Publish filled',k.publish_filled],['Amount filled',k.amount_filled],['Attachments',k.attachments],['Specs',k.specs]];
 document.getElementById('kpis').innerHTML=cards.map(([a,b])=>`<div class=card><div class=small>${a}</div><div style="font-size:24px">${b}</div></div>`).join('');
 const top=await get('/api/top-institutions');
 const dep=await get('/api/dependency');
 const rel=await get('/api/relationships?limit=20');
 document.getElementById('samples').innerHTML='<h4>Top institutions</h4>'+table(top)+'<h4>Dependency</h4>'+table(dep);
 document.getElementById('rels').innerHTML=table(rel);
 await loadCalls();
}
async function loadCalls(){
 const p=new URLSearchParams({from:from.value,to:to.value,institution:institution.value,status:status.value,category:category.value,limit:'50'});
 const rows=await get('/api/calls?'+p.toString());
 document.getElementById('calls').innerHTML=table(rows);
}
function downloadCsv(){
 const p=new URLSearchParams({from:from.value,to:to.value,institution:institution.value,status:status.value,category:category.value,limit:'2000'});
 window.open('/api/calls.csv?'+p.toString(),'_blank');
}
load();
</script>
</body></html>
"""


@app.get('/api/kpis')
def api_kpis():
    q = """
    select
      (select count(*) from opportunities) as opportunities_total,
      (select count(*) from opportunities where publish_at is not null) as publish_filled,
      (select count(*) from opportunities where amount is not null) as amount_filled,
      (select count(*) from opportunity_attachments) as attachments,
      (select count(*) from attachment_specs) as specs
    """
    return fetch_all(q)[0]


@app.get('/api/top-institutions')
def api_top_inst(limit: int = 10):
    q = """
    select institution, sum(calls)::bigint as calls, round(sum(total_amount)::numeric,2) as total_amount
    from analytics.v_call_aggregates_by_filters
    where institution <> 'unknown'
    group by 1
    order by 2 desc
    limit %s
    """
    return fetch_all(q, (limit,))


@app.get('/api/dependency')
def api_dependency(limit: int = 10):
    q = """
    select company, institution, wins, wins_dependency_pct
    from analytics.v_company_dependency
    where company <> 'unknown' and institution <> 'unknown'
    order by wins desc
    limit %s
    """
    return fetch_all(q, (limit,))


@app.get('/api/relationships')
def api_relationships(limit: int = 20):
    q = """
    select institution, company, wins, amount_sum, first_seen, last_seen
    from analytics.v_institution_company_relationships
    where institution <> 'unknown' and company <> 'unknown'
    order by wins desc
    limit %s
    """
    return fetch_all(q, (limit,))


def _calls_rows(from_: str, to: str, institution: str, status: str, category: str, limit: int):
    q = """
    select external_id, title, buyer_name, category, status, publish_at, deadline_at, source_url
    from analytics.fact_calls
    where (publish_at is null or (publish_at >= %s and publish_at < %s))
      and (%s = '' or coalesce(buyer_name,'') ilike '%%' || %s || '%%')
      and (%s = '' or coalesce(status,'') = %s)
      and (%s = '' or coalesce(category,'') ilike '%%' || %s || '%%')
    order by publish_at desc nulls last
    limit %s
    """
    return fetch_all(q, (from_, to, institution, institution, status, status, category, category, limit))


@app.get('/api/calls')
def api_calls(
    from_: str = Query("2025-01-01", alias='from'),
    to: str = Query("2026-01-01"),
    institution: str = "",
    status: str = "",
    category: str = "",
    limit: int = 50,
):
    return _calls_rows(from_, to, institution, status, category, limit)


@app.get('/api/calls.csv', response_class=PlainTextResponse)
def api_calls_csv(
    from_: str = Query("2025-01-01", alias='from'),
    to: str = Query("2026-01-01"),
    institution: str = "",
    status: str = "",
    category: str = "",
    limit: int = 2000,
):
    rows = _calls_rows(from_, to, institution, status, category, limit)
    if not rows:
        return ""
    out = io.StringIO()
    w = csv.DictWriter(out, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)
    return out.getvalue()
