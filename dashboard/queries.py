"""Consultas del dashboard. Todo se lee de la capa Gold (y audit para calidad).

Funciones puras: reciben una conexión y devuelven un DataFrame, así se pueden
probar sin Streamlit.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def _df(conn: Any, sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c[0] for c in cur.description] if cur.description else []
        return pd.DataFrame(cur.fetchall(), columns=cols)


def available_dates(conn: Any) -> list:
    df = _df(conn, "SELECT DISTINCT full_date FROM gold.mart_category_daily ORDER BY full_date DESC")
    return df["full_date"].tolist()


def mart(conn: Any, start, end) -> pd.DataFrame:
    """Mart país x categoría x día en el rango pedido."""
    df = _df(
        conn,
        """
        SELECT full_date, site_id, country, category_id, category_name, items, sellers,
               avg_price_usd, median_price_usd, p25_price_usd, p75_price_usd,
               pct_free_shipping, avg_discount_pct
        FROM gold.mart_category_daily
        WHERE full_date BETWEEN %(start)s AND %(end)s
        ORDER BY full_date, site_id, category_id
        """,
        {"start": start, "end": end},
    )
    numeric = [
        "avg_price_usd",
        "median_price_usd",
        "p25_price_usd",
        "p75_price_usd",
        "pct_free_shipping",
        "avg_discount_pct",
    ]
    df[numeric] = df[numeric].apply(pd.to_numeric, errors="coerce")
    # Nombre común entre países (Notebooks / Laptops, Tenis / Tênis...) para comparar.
    df["category_group"] = (
        df["category_id"]
        .str[3:]
        .map(
            {"1055": "Celulares", "3813": "Accesorios celulares", "1652": "Notebooks", "109027": "Zapatillas"}
        )
        .fillna(df["category_name"])
    )
    return df


def kpis(conn: Any, day, sites: list[str]) -> pd.DataFrame:
    """KPIs del día y del día anterior (para mostrar la variación)."""
    df = _df(
        conn,
        """
        SELECT d.full_date,
               count(*)                                         AS items,
               count(DISTINCT f.seller_id)                      AS sellers,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY f.price_usd) AS median_price_usd,
               100.0 * avg(CASE WHEN f.free_shipping THEN 1 ELSE 0 END)  AS pct_free_shipping
        FROM gold.fact_item_daily f
        JOIN gold.dim_date d USING (date_key)
        WHERE d.full_date IN (%(day)s::date, %(day)s::date - 1)
          AND f.site_id = ANY(%(sites)s)
        GROUP BY d.full_date
        ORDER BY d.full_date DESC
        """,
        {"day": day, "sites": sites},
    )
    for c in ("median_price_usd", "pct_free_shipping"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def price_changes(conn: Any, day, sites: list[str], limit: int = 15) -> pd.DataFrame:
    df = _df(
        conn,
        """
        SELECT p.item_id, s.country, c.category_name, p.prev_price_usd, p.price_usd, p.pct_change
        FROM gold.vw_price_changes p
        JOIN gold.dim_site s USING (site_id)
        JOIN gold.dim_category c USING (category_id)
        WHERE p.full_date = %(day)s AND p.pct_change IS NOT NULL AND p.site_id = ANY(%(sites)s)
        ORDER BY p.pct_change ASC
        LIMIT %(limit)s
        """,
        {"day": day, "sites": sites, "limit": limit},
    )
    for c in ("prev_price_usd", "price_usd", "pct_change"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def top_sellers(conn: Any, day, sites: list[str], limit: int = 10) -> pd.DataFrame:
    return _df(
        conn,
        """
        SELECT v.nickname, s.country, v.is_official_store, count(*) AS publicaciones,
               round(avg(f.price_usd), 2) AS precio_promedio_usd
        FROM gold.fact_item_daily f
        JOIN gold.dim_seller v ON v.seller_sk = f.seller_sk
        JOIN gold.dim_site s ON s.site_id = f.site_id
        JOIN gold.dim_date d USING (date_key)
        WHERE d.full_date = %(day)s AND f.site_id = ANY(%(sites)s)
        GROUP BY v.nickname, s.country, v.is_official_store
        ORDER BY publicaciones DESC
        LIMIT %(limit)s
        """,
        {"day": day, "sites": sites, "limit": limit},
    )


def seller_history(conn: Any, sites: list[str]) -> pd.DataFrame:
    """Vendedores con más de una versión en la dimensión SCD tipo 2."""
    return _df(
        conn,
        """
        SELECT v.seller_id, s.country, v.seller_sk, v.nickname, v.is_official_store,
               v.valid_from, NULLIF(v.valid_to, DATE '9999-12-31') AS valid_to, v.is_current
        FROM gold.dim_seller v
        JOIN gold.dim_site s USING (site_id)
        WHERE v.site_id = ANY(%(sites)s)
          AND v.seller_id IN (SELECT seller_id FROM gold.dim_seller GROUP BY seller_id HAVING count(*) > 1)
        ORDER BY s.country, v.seller_id, v.valid_from
        """,
        {"sites": sites},
    )


def dq_results(conn: Any, day) -> pd.DataFrame:
    return _df(
        conn,
        """
        SELECT layer, check_name, dimension, severity, passed, observed, threshold, message
        FROM audit.vw_dq_latest
        WHERE run_date = %(day)s
        ORDER BY layer DESC, passed, check_name
        """,
        {"day": day},
    )


def quarantine_by_reason(conn: Any, start, end) -> pd.DataFrame:
    return _df(
        conn,
        """
        SELECT snapshot_date, reason, count(*) AS registros
        FROM silver.items_quarantine
        WHERE snapshot_date BETWEEN %(start)s AND %(end)s
        GROUP BY 1, 2 ORDER BY 1, 2
        """,
        {"start": start, "end": end},
    )


def pipeline_volume(conn: Any, start, end) -> pd.DataFrame:
    """Filas que entraron a Bronze vs. las que quedaron en Silver, por día."""
    return _df(
        conn,
        """
        SELECT run_date,
               sum(rows_in)  FILTER (WHERE step = 'bronze_extract') AS bronze,
               sum(rows_out) FILTER (WHERE step = 'silver_build')   AS silver
        FROM (
            SELECT DISTINCT ON (run_date, step, site_id) run_date, step, site_id, rows_in, rows_out
            FROM audit.pipeline_runs
            WHERE status = 'success' AND run_date BETWEEN %(start)s AND %(end)s
            ORDER BY run_date, step, site_id, finished_at DESC
        ) last_runs
        GROUP BY run_date
        ORDER BY run_date
        """,
        {"start": start, "end": end},
    )
