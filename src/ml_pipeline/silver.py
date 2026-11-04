"""Capa SILVER: datos limpios, tipados, deduplicados y con moneda normalizada.

Flujo por (fecha, sitio):
  bronze (jsonb) --chunks--> pandas: aplanar + reglas + USD
                               |-> válidos  -> stage temporal -> ROW_NUMBER -> MERGE
                               |-> inválidos -> silver.items_quarantine (con motivo)
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import pandas as pd

from ml_pipeline import audit, db
from ml_pipeline.config import Settings
from ml_pipeline.transform import normalize

log = logging.getLogger(__name__)

STAGE_COLUMNS = [
    "record_seq",
    *normalize.SNAPSHOT_COLUMNS[:7],
    "seller_nickname",
    *normalize.SNAPSHOT_COLUMNS[7:],
]


def _rates_for(conn: Any, run_date: date) -> dict[str, float]:
    # Tasa vigente: la más reciente con fecha <= fecha del snapshot.
    rows = db.fetch_all(
        conn,
        """
        SELECT DISTINCT ON (currency_id) currency_id, usd_per_unit
        FROM silver.fx_rates
        WHERE rate_date <= %(d)s
        ORDER BY currency_id, rate_date DESC
        """,
        {"d": run_date},
    )
    return {cur: float(rate) for cur, rate in rows}


def build_silver(
    site_id: str, run_date: date, run_id: str, settings: Settings, chunk_size: int = 5_000
) -> dict[str, Any]:
    started = audit.now_utc()
    rows_in = valid_total = rejected_total = 0
    reasons: dict[str, int] = {}

    with db.connect(settings.warehouse_dsn) as conn:
        rates = _rates_for(conn, run_date)
        if not rates:
            raise RuntimeError(f"No hay tasas de cambio cargadas para {run_date}")

        db.execute(conn, db.read_sql("silver/create_stage.sql"))
        db.execute(
            conn,
            "DELETE FROM silver.items_quarantine WHERE snapshot_date = %(d)s AND site_id = %(s)s",
            {"d": run_date, "s": site_id},
        )

        reader_sql = """
            SELECT record_seq, search_term, payload::text
            FROM bronze.ml_items_raw
            WHERE ingestion_date = %(d)s AND site_id = %(s)s
            ORDER BY record_seq
        """
        # Lectura en chunks con cursor del servidor: la memoria usada no depende
        # del tamaño de la partición (mismo principio que procesar un CSV de 20 GB).
        for chunk in db.iter_chunks(conn, reader_sql, {"d": run_date, "s": site_id}, chunk_size):
            rows_in += len(chunk)
            df = normalize.flatten_items(chunk)
            valid, rejected = normalize.split_valid_rejected(df, set(rates))
            valid = normalize.add_usd_amount(valid, rates)
            valid = valid.assign(snapshot_date=run_date.isoformat())

            db.copy_dataframe(conn, valid, "stg_items_raw", STAGE_COLUMNS)
            if not rejected.empty:
                q = pd.DataFrame(
                    {
                        "snapshot_date": run_date.isoformat(),
                        "site_id": site_id,
                        "item_id": rejected["item_id"],
                        "reason": rejected["reason"],
                        "payload": rejected["raw_payload"],
                        "_run_id": run_id,
                    }
                )
                db.copy_dataframe(conn, q, "silver.items_quarantine", list(q.columns))
                for r, n in rejected["reason"].value_counts().items():
                    reasons[str(r)] = reasons.get(str(r), 0) + int(n)
            valid_total += len(valid)
            rejected_total += len(rejected)

        db.execute(
            conn,
            db.read_sql("silver/merge_items.sql"),
            {"snapshot_date": run_date, "site_id": site_id, "run_id": run_id},
        )
        deduped = db.fetch_one(
            conn,
            "SELECT count(*) FROM silver.item_snapshots WHERE snapshot_date=%(d)s AND site_id=%(s)s",
            {"d": run_date, "s": site_id},
        )[0]

        metrics = {
            "site_id": site_id,
            "rows_in": rows_in,
            "valid": valid_total,
            "rejected": rejected_total,
            "duplicates_removed": valid_total - deduped,
            "rows_out": deduped,
            "reject_reasons": reasons,
        }
        audit.log_step(
            conn,
            run_id=run_id,
            step="silver_build",
            site_id=site_id,
            run_date=run_date,
            started_at=started,
            rows_in=rows_in,
            rows_out=deduped,
            details=metrics,
        )
    log.info("Silver %s %s: %s", site_id, run_date, metrics)
    return metrics
