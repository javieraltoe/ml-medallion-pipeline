"""Capa BRONZE: aterrizar los datos crudos sin modificarlos.

Patrón de idempotencia: "delete + insert por partición". La partición es
(ingestion_date, site_id). Re-ejecutar el mismo día reemplaza la partición
completa, nunca duplica.

Orden deliberado:
  1. se guarda el crudo (archivo JSONL en el lake + tabla bronze)
  2. recién después se valida el contrato
Así, si el origen cambia el esquema, conservamos la evidencia para analizarla
pero el DAG falla y no contamina Silver.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ml_pipeline import audit, db
from ml_pipeline.config import SITES, Settings
from ml_pipeline.extract import sample_source
from ml_pipeline.extract.ml_client import MercadoLibreClient
from ml_pipeline.quality import contracts

log = logging.getLogger(__name__)


def _fetch(site_id: str, run_date: date, settings: Settings) -> list[tuple[str, dict[str, Any]]]:
    rows: list[tuple[str, dict[str, Any]]] = []
    client = MercadoLibreClient(settings) if settings.source_mode == "api" else None
    for term in settings.search_terms:
        if client:
            items = client.search(site_id, term, settings.max_items_per_term)
        else:
            items = sample_source.search(site_id, term, settings.max_items_per_term, run_date)
        rows.extend((term, item) for item in items)
    return rows


def lake_path(settings: Settings, dataset: str, site_id: str, run_date: date) -> Path:
    """Particionado estilo Hive: fácil de leer luego con Spark/Databricks/Athena."""
    return (
        settings.lake_root
        / "bronze"
        / "mercadolibre"
        / dataset
        / f"site_id={site_id}"
        / f"ingestion_date={run_date.isoformat()}"
        / "part-000.jsonl"
    )


def extract_to_bronze(site_id: str, run_date: date, run_id: str, settings: Settings) -> dict[str, Any]:
    if site_id not in SITES:
        raise ValueError(f"Sitio no soportado: {site_id}")
    started = audit.now_utc()
    records = _fetch(site_id, run_date, settings)
    log.info("%s %s: %d registros extraídos (%s)", site_id, run_date, len(records), settings.source_mode)

    # 1) Data lake: archivo inmutable por partición (overwrite = idempotente)
    path = lake_path(settings, "search_items", site_id, run_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for seq, (term, item) in enumerate(records):
            fh.write(
                json.dumps({"search_term": term, "record_seq": seq, "item": item}, ensure_ascii=False) + "\n"
            )

    # 2) Tabla bronze
    df = pd.DataFrame(
        {
            "ingestion_date": run_date.isoformat(),
            "site_id": site_id,
            "search_term": [t for t, _ in records],
            "record_seq": range(len(records)),
            "item_id": [i.get("id") for _, i in records],
            "payload": [json.dumps(i, ensure_ascii=False) for _, i in records],
            "source_mode": settings.source_mode,
            "source_file": str(path),
            "run_id": run_id,
        }
    )
    df["payload_hash"] = df["payload"].map(lambda p: hashlib.sha256(p.encode()).hexdigest())

    contract = contracts.load_contract("ml_search_item")
    report = contracts.validate_records([i for _, i in records], contract)

    with db.connect(settings.warehouse_dsn) as conn:
        deleted = db.execute(
            conn,
            "DELETE FROM bronze.ml_items_raw WHERE ingestion_date = %(d)s AND site_id = %(s)s",
            {"d": run_date, "s": site_id},
        )
        loaded = db.copy_dataframe(
            conn,
            df,
            "bronze.ml_items_raw",
            [
                "ingestion_date",
                "site_id",
                "search_term",
                "record_seq",
                "item_id",
                "payload",
                "payload_hash",
                "source_mode",
                "source_file",
                "run_id",
            ],
        )
        audit.log_step(
            conn,
            run_id=run_id,
            step="bronze_extract",
            site_id=site_id,
            run_date=run_date,
            started_at=started,
            rows_in=len(records),
            rows_out=loaded,
            details={"replaced_rows": deleted, "lake_file": str(path), "contract": report.as_dict()},
        )

    # 3) Contrato: falla DESPUÉS de guardar el crudo
    contracts.enforce(report, contract["max_violation_ratio"])
    return {"site_id": site_id, "rows": loaded, "replaced": deleted, "contract": report.as_dict()}


def load_fx_rates(run_date: date, run_id: str, settings: Settings) -> dict[str, float]:
    """Tasas de cambio del día. Se guarda crudo en bronze y normalizado en silver (MERGE)."""
    started = audit.now_utc()
    currencies = sorted({SITES[s]["currency"] for s in settings.sites} | {"USD"})
    client = MercadoLibreClient(settings) if settings.source_mode == "api" else None
    rates: dict[str, float] = {}
    for cur in currencies:
        rates[cur] = client.usd_rate(cur) if client else sample_source.usd_rate(cur, run_date)

    with db.connect(settings.warehouse_dsn) as conn:
        db.execute(
            conn,
            "DELETE FROM bronze.fx_rates_raw WHERE rate_date = %(d)s",
            {"d": run_date},
        )
        for cur, rate in rates.items():
            params = {
                "d": run_date,
                "c": cur,
                "r": rate,
                "src": settings.source_mode,
                "run": run_id,
                "p": json.dumps({"from": cur, "to": "USD", "ratio": rate}),
            }
            db.execute(
                conn,
                """INSERT INTO bronze.fx_rates_raw (rate_date, currency_id, payload, source_mode, run_id)
                   VALUES (%(d)s, %(c)s, %(p)s, %(src)s, %(run)s)""",
                params,
            )
            db.execute(conn, db.read_sql("silver/merge_fx_rates.sql"), params)
        audit.log_step(
            conn,
            run_id=run_id,
            step="fx_rates",
            run_date=run_date,
            started_at=started,
            rows_out=len(rates),
            details=rates,
        )
    return rates
