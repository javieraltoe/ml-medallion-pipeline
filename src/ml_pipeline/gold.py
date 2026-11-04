"""Capa GOLD: modelo estrella y data marts listos para BI / análisis (ELT en SQL)."""

from __future__ import annotations

import csv
import logging
from datetime import date
from pathlib import Path
from typing import Any

from ml_pipeline import audit, db
from ml_pipeline.config import SITES, Settings
from ml_pipeline.extract.ml_client import MercadoLibreClient

log = logging.getLogger(__name__)

SEEDS_DIR = Path(__file__).resolve().parents[2] / "seeds"


def _load_dimensions_from_seeds(conn: Any, settings: Settings) -> None:
    for site_id, meta in SITES.items():
        db.execute(
            conn,
            """INSERT INTO gold.dim_site (site_id, country, country_code, currency_id)
               VALUES (%(s)s, %(c)s, %(cc)s, %(cur)s)
               ON CONFLICT (site_id) DO UPDATE SET country = EXCLUDED.country,
                   country_code = EXCLUDED.country_code, currency_id = EXCLUDED.currency_id""",
            {"s": site_id, "c": meta["country"], "cc": meta["country_code"], "cur": meta["currency"]},
        )
    with (SEEDS_DIR / "categories.csv").open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            db.execute(
                conn,
                """INSERT INTO gold.dim_category (category_id, site_id, category_name)
                   VALUES (%(category_id)s, %(site_id)s, %(category_name)s)
                   ON CONFLICT (category_id) DO UPDATE SET category_name = EXCLUDED.category_name""",
                row,
            )


def _enrich_unknown_categories(conn: Any, settings: Settings) -> None:
    """En modo api, completa nombres de categorías nuevas con /categories/{id}."""
    if settings.source_mode != "api":
        return
    client = MercadoLibreClient(settings)
    for (category_id,) in db.fetch_all(
        conn,
        "SELECT category_id FROM gold.dim_category WHERE category_name LIKE %(p)s",
        {"p": "Sin clasificar%"},
    ):
        try:
            name = client.category(category_id)["name"]
        except Exception as exc:  # no frenar el pipeline por un nombre
            log.warning("No se pudo obtener la categoría %s: %s", category_id, exc)
            continue
        db.execute(
            conn,
            "UPDATE gold.dim_category SET category_name=%(n)s WHERE category_id=%(c)s",
            {"n": name, "c": category_id},
        )


def build_gold(run_date: date, run_id: str, settings: Settings) -> dict[str, Any]:
    started = audit.now_utc()
    with db.connect(settings.warehouse_dsn) as conn:
        _load_dimensions_from_seeds(conn, settings)
        db.execute(conn, db.read_sql("gold/build_gold.sql"), {"d": run_date, "run_id": run_id})
        _enrich_unknown_categories(conn, settings)
        facts = db.fetch_one(
            conn,
            "SELECT count(*) FROM gold.fact_item_daily WHERE date_key = TO_CHAR(%(d)s::date,'YYYYMMDD')::int",
            {"d": run_date},
        )[0]
        marts = db.fetch_one(
            conn, "SELECT count(*) FROM gold.mart_category_daily WHERE full_date = %(d)s", {"d": run_date}
        )[0]
        metrics = {"fact_rows": facts, "mart_rows": marts}
        audit.log_step(
            conn,
            run_id=run_id,
            step="gold_build",
            run_date=run_date,
            started_at=started,
            rows_out=facts,
            details=metrics,
        )
    return metrics
