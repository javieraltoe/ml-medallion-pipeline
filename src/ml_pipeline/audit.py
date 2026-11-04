"""Registro de ejecuciones (observabilidad básica: qué corrió, cuántas filas, cuánto tardó)."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import Any

from ml_pipeline import db


def now_utc() -> datetime:
    return datetime.now(UTC)


def log_step(
    conn: Any,
    *,
    run_id: str,
    step: str,
    run_date: date,
    started_at: datetime,
    status: str = "success",
    site_id: str | None = None,
    rows_in: int | None = None,
    rows_out: int | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    db.execute(
        conn,
        """
        INSERT INTO audit.pipeline_runs
            (run_id, step, site_id, run_date, status, rows_in, rows_out, details, started_at)
        VALUES (%(run_id)s, %(step)s, %(site_id)s, %(run_date)s, %(status)s,
                %(rows_in)s, %(rows_out)s, %(details)s, %(started_at)s)
        """,
        {
            "run_id": run_id,
            "step": step,
            "site_id": site_id,
            "run_date": run_date,
            "status": status,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "details": json.dumps(details or {}, default=str),
            "started_at": started_at,
        },
    )
