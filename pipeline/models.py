from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class Opportunity:
    source: str
    external_id: str
    title: str
    description: str
    buyer_name: str | None
    publish_at: datetime | None
    deadline_at: datetime | None
    status: str
    amount: float | None
    currency: str | None
    category: str | None
    department: str | None
    source_url: str
    raw_hash: str


@dataclass
class Assignment:
    business_id: str
    score: float
    reasons: dict
