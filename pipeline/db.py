from __future__ import annotations

import os
from contextlib import contextmanager

import psycopg


def dsn() -> str:
    return os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/rupe")


@contextmanager
def get_conn():
    conn = psycopg.connect(dsn())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
