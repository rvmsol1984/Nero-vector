"""Read-only PostgreSQL access for the vector-ui backend.

Connects to the same `nero_vector` database that `vector-ingest` writes to
and exposes a tiny fetch_all / fetch_one helper layered on a threaded
connection pool. Every connection has its session pinned to UTC so that
TIMESTAMPTZ values surface to FastAPI in UTC regardless of host tz.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg2
import psycopg2.extras
import psycopg2.pool

logger = logging.getLogger(__name__)

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def init_pool() -> None:
    global _pool
    if _pool is not None:
        return

    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    dbname = os.environ.get("POSTGRES_DB", "nero_vector")
    user = os.environ.get("POSTGRES_USER", "nero_vector")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    maxconn = int(os.environ.get("VECTOR_UI_DB_POOL", "5"))

    logger.info("initializing vector-ui db pool host=%s db=%s", host, dbname)
    _pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=maxconn,
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        application_name="vector-ui",
        options="-c timezone=UTC",
    )


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None


@contextmanager
def _cursor() -> Iterator[psycopg2.extras.RealDictCursor]:
    if _pool is None:
        raise RuntimeError("db pool not initialized")
    conn = _pool.getconn()
    try:
        # Every borrowed connection runs in autocommit; all queries here are
        # read-only SELECTs so there's no transactional state to manage.
        if not conn.autocommit:
            conn.rollback()
            conn.autocommit = True
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
    finally:
        _pool.putconn(conn)


def fetch_all(sql: str, params: tuple[Any, ...] | None = None) -> list[dict]:
    with _cursor() as cur:
        cur.execute(sql, params or ())
        return [dict(row) for row in cur.fetchall()]


def fetch_one(sql: str, params: tuple[Any, ...] | None = None) -> dict | None:
    with _cursor() as cur:
        cur.execute(sql, params or ())
        row = cur.fetchone()
        return dict(row) if row else None


def execute_returning(sql: str, params: tuple[Any, ...] | None = None) -> dict | None:
    """Execute a mutation with RETURNING and hand back the first row as a dict.

    Used by the INKY receiver to INSERT ... RETURNING id. Because the
    shared pool runs every connection in autocommit, no explicit commit
    is needed -- the row is durable the moment execute() returns.
    """
    with _cursor() as cur:
        cur.execute(sql, params or ())
        try:
            row = cur.fetchone()
        except psycopg2.ProgrammingError:
            row = None
    return dict(row) if row else None
