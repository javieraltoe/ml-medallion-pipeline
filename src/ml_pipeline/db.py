"""Acceso mínimo a Postgres (warehouse).

Se usa psycopg2 directo (en vez de un ORM) para que el SQL sea explícito y
fácil de leer en una revisión de código.
"""

from __future__ import annotations

import io
import logging
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2

log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"


def read_sql(relative_path: str) -> str:
    return (SQL_DIR / relative_path).read_text()


@contextmanager
def connect(dsn: str) -> Iterator[Any]:
    """Conexión transaccional: commit si todo sale bien, rollback si hay error."""
    conn = psycopg2.connect(dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute(conn: Any, sql: str, params: dict[str, Any] | None = None) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount


def fetch_all(conn: Any, sql: str, params: dict[str, Any] | None = None) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def fetch_one(conn: Any, sql: str, params: dict[str, Any] | None = None) -> tuple | None:
    rows = fetch_all(conn, sql, params)
    return rows[0] if rows else None


def iter_chunks(
    conn: Any, sql: str, params: dict[str, Any] | None, chunk_size: int = 5_000
) -> Iterator[list[tuple]]:
    """Cursor del lado del servidor: lee por bloques sin cargar todo en memoria."""
    with conn.cursor(name="chunked_reader") as cur:
        cur.itersize = chunk_size
        cur.execute(sql, params)
        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                break
            yield rows


def copy_dataframe(conn: Any, df: pd.DataFrame, table: str, columns: Sequence[str]) -> int:
    """Carga masiva con COPY (mucho más rápido que INSERT fila a fila)."""
    if df.empty:
        return 0
    buf = io.StringIO()
    df.loc[:, list(columns)].to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    cols = ", ".join(columns)
    with conn.cursor() as cur:
        cur.copy_expert(f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    return len(df)


def run_migrations(dsn: str) -> None:
    with connect(dsn) as conn:
        for path in sorted((SQL_DIR / "ddl").glob("*.sql")):
            log.info("Aplicando %s", path.name)
            execute(conn, path.read_text())
