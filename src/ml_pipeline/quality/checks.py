"""Framework liviano de calidad de datos.

Cada control es una consulta SQL que devuelve UN número ("observed") que se
compara contra un umbral. Los resultados se guardan en audit.dq_results y,
si falla algún control de severidad "error", la tarea de Airflow falla
=> no se publican datos deficientes.

Dimensiones cubiertas: unicidad, completitud, validez, consistencia,
integridad referencial, reconciliación, volumen y frescura.
"""

from __future__ import annotations

import logging
import operator
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any

from ml_pipeline import db
from ml_pipeline.config import SITES, Settings

log = logging.getLogger(__name__)

OPS: dict[str, Callable[[float, float], bool]] = {
    "eq": operator.eq,
    "lte": operator.le,
    "gte": operator.ge,
}


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class Check:
    name: str
    layer: str
    dimension: str
    sql: str
    op: str
    threshold: Callable[[Settings], float]
    severity: str = "error"
    description: str = ""
    pass_if_null: bool = False  # ej. sin histórico todavía para comparar volumen


def _site_currency_values() -> str:
    return ", ".join(f"('{s}', '{m['currency']}')" for s, m in SITES.items())


SILVER_CHECKS: list[Check] = [
    Check(
        "silver_all_sites_loaded",
        "silver",
        "completeness",
        "SELECT count(DISTINCT site_id) FROM silver.item_snapshots WHERE snapshot_date = %(d)s",
        "gte",
        lambda s: len(s.sites),
        description="Llegaron datos de todos los países configurados",
    ),
    Check(
        "silver_unique_item_per_day",
        "silver",
        "uniqueness",
        """SELECT count(*) FROM (
               SELECT item_id FROM silver.item_snapshots WHERE snapshot_date = %(d)s
               GROUP BY item_id HAVING count(*) > 1) dup""",
        "eq",
        lambda s: 0,
        description="Una sola fila por publicación y día",
    ),
    Check(
        "silver_price_usd_not_null",
        "silver",
        "completeness",
        "SELECT count(*) FROM silver.item_snapshots WHERE snapshot_date = %(d)s AND price_usd IS NULL",
        "eq",
        lambda s: 0,
        description="Todo precio tiene su equivalente en USD",
    ),
    Check(
        "silver_price_positive",
        "silver",
        "validity",
        "SELECT count(*) FROM silver.item_snapshots WHERE snapshot_date = %(d)s AND price <= 0",
        "eq",
        lambda s: 0,
        description="Precios estrictamente positivos",
    ),
    Check(
        "silver_currency_consistent_with_site",
        "silver",
        "consistency",
        f"""SELECT count(*) FROM silver.item_snapshots t
            JOIN (VALUES {_site_currency_values()}) AS sc(site_id, currency) USING (site_id)
            WHERE t.snapshot_date = %(d)s AND t.currency_id NOT IN (sc.currency, 'USD')""",
        "eq",
        lambda s: 0,
        description="Moneda = moneda local del sitio o USD",
    ),
    Check(
        "silver_reconciliation_with_bronze",
        "silver",
        "reconciliation",
        """SELECT (SELECT count(DISTINCT item_id) FROM bronze.ml_items_raw
                   WHERE ingestion_date = %(d)s AND item_id IS NOT NULL)
                - (SELECT count(DISTINCT item_id) FROM (
                       SELECT item_id FROM silver.item_snapshots WHERE snapshot_date = %(d)s
                       UNION
                       SELECT item_id FROM silver.items_quarantine
                       WHERE snapshot_date = %(d)s AND item_id IS NOT NULL) u)""",
        "eq",
        lambda s: 0,
        description="Todo id de Bronze está en Silver o en cuarentena (nada se pierde)",
    ),
    Check(
        "silver_quarantine_ratio",
        "silver",
        "validity",
        """SELECT (SELECT count(*) FROM silver.items_quarantine WHERE snapshot_date = %(d)s)::numeric
                  / NULLIF((SELECT count(*) FROM bronze.ml_items_raw WHERE ingestion_date = %(d)s), 0)""",
        "lte",
        lambda s: s.max_quarantine_ratio,
        description="Porcentaje de registros rechazados acotado",
    ),
    Check(
        "silver_volume_vs_7d_avg",
        "silver",
        "volume",
        """WITH daily AS (
               SELECT site_id, snapshot_date, count(*) AS n
               FROM silver.item_snapshots
               WHERE snapshot_date BETWEEN %(d)s::date - 7 AND %(d)s
               GROUP BY site_id, snapshot_date),
           cmp AS (
               SELECT t.site_id, t.n::numeric / NULLIF(AVG(h.n), 0) AS ratio
               FROM daily t JOIN daily h
                 ON h.site_id = t.site_id AND h.snapshot_date < t.snapshot_date
               WHERE t.snapshot_date = %(d)s
               GROUP BY t.site_id, t.n)
           SELECT MIN(ratio) FROM cmp""",
        "gte",
        lambda s: 1 - s.volume_drop_threshold,
        pass_if_null=True,
        description="Filas del día vs promedio de los 7 días previos (detecta caídas bruscas)",
    ),
    Check(
        "silver_fx_rate_fresh",
        "silver",
        "freshness",
        """SELECT count(DISTINCT s.currency_id) FROM silver.item_snapshots s
           LEFT JOIN silver.fx_rates r ON r.currency_id = s.currency_id AND r.rate_date = s.snapshot_date
           WHERE s.snapshot_date = %(d)s AND r.currency_id IS NULL""",
        "eq",
        lambda s: 0,
        severity="warn",
        description="Se usó una tasa del mismo día (si no, se aplicó la última disponible)",
    ),
]

GOLD_CHECKS: list[Check] = [
    Check(
        "gold_fact_matches_silver_rows",
        "gold",
        "reconciliation",
        """SELECT abs((SELECT count(*) FROM gold.fact_item_daily
                       WHERE date_key = TO_CHAR(%(d)s::date, 'YYYYMMDD')::int)
                    - (SELECT count(*) FROM silver.item_snapshots WHERE snapshot_date = %(d)s))""",
        "eq",
        lambda s: 0,
        description="Mismas filas en Gold que en Silver",
    ),
    Check(
        "gold_fact_matches_silver_amounts",
        "gold",
        "reconciliation",
        """SELECT abs(COALESCE((SELECT sum(price_usd) FROM gold.fact_item_daily
                                WHERE date_key = TO_CHAR(%(d)s::date, 'YYYYMMDD')::int), 0)
                    - COALESCE((SELECT sum(price_usd) FROM silver.item_snapshots
                                WHERE snapshot_date = %(d)s), 0))""",
        "lte",
        lambda s: 0.01,
        description="Mismos importes en USD en Gold que en Silver",
    ),
    Check(
        "gold_fact_referential_integrity",
        "gold",
        "integrity",
        """SELECT count(*) FROM gold.fact_item_daily f
           LEFT JOIN gold.dim_category c ON c.category_id = f.category_id
           LEFT JOIN gold.dim_seller   v ON v.seller_id   = f.seller_id
           LEFT JOIN gold.dim_site     t ON t.site_id     = f.site_id
           WHERE f.date_key = TO_CHAR(%(d)s::date, 'YYYYMMDD')::int
             AND (c.category_id IS NULL OR v.seller_id IS NULL OR t.site_id IS NULL)""",
        "eq",
        lambda s: 0,
        description="Toda fila de hechos apunta a dimensiones existentes",
    ),
    Check(
        "gold_mart_country_coverage",
        "gold",
        "completeness",
        "SELECT count(DISTINCT site_id) FROM gold.mart_category_daily WHERE full_date = %(d)s",
        "gte",
        lambda s: len(s.sites),
        description="El mart tiene datos de todos los países configurados",
    ),
]


def run_checks(layer: str, run_date: date, run_id: str, settings: Settings) -> list[dict[str, Any]]:
    checks = SILVER_CHECKS if layer == "silver" else GOLD_CHECKS
    results: list[dict[str, Any]] = []
    with db.connect(settings.warehouse_dsn) as conn:
        for chk in checks:
            raw = db.fetch_one(conn, chk.sql, {"d": run_date})[0]
            threshold = float(chk.threshold(settings))
            observed = None if raw is None else float(raw)
            if observed is None:
                passed = chk.pass_if_null
                message = "Sin datos para comparar" if passed else "El control no devolvió valor"
            else:
                passed = OPS[chk.op](round(observed, 6), threshold)
                message = f"{chk.description}: observado={observed:g} {chk.op} {threshold:g}"
            db.execute(
                conn,
                """INSERT INTO audit.dq_results (run_id, run_date, layer, check_name, dimension, severity,
                                                 passed, observed, threshold, message)
                   VALUES (%(run_id)s, %(d)s, %(layer)s, %(name)s, %(dim)s, %(sev)s, %(passed)s,
                           %(obs)s, %(thr)s, %(msg)s)""",
                {
                    "run_id": run_id,
                    "d": run_date,
                    "layer": layer,
                    "name": chk.name,
                    "dim": chk.dimension,
                    "sev": chk.severity,
                    "passed": passed,
                    "obs": observed,
                    "thr": threshold,
                    "msg": message,
                },
            )
            results.append(
                {"check": chk.name, "severity": chk.severity, "passed": passed, "message": message}
            )
            (log.info if passed else log.warning)("[DQ %s] %s -> %s", layer, chk.name, message)

    failed = [r for r in results if not r["passed"] and r["severity"] == "error"]
    if failed:
        # Se guardan los resultados (commit) ANTES de fallar, para poder auditarlos.
        raise DataQualityError(
            f"{len(failed)} control(es) de calidad fallaron en {layer}: "
            + "; ".join(f"{r['check']} ({r['message']})" for r in failed)
        )
    return results
