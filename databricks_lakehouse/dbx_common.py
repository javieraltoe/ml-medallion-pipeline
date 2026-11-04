"""Utilidades compartidas por los notebooks de Databricks.

Funciones puras (sin Spark) para que se puedan testear localmente con pytest.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Argentina/Buenos_Aires")
SITES = {
    "MLA": {"country": "Argentina", "country_code": "AR", "currency": "ARS"},
    "MLB": {"country": "Brasil", "country_code": "BR", "currency": "BRL"},
    "MLM": {"country": "México", "country_code": "MX", "currency": "MXN"},
}
SEARCH_TERMS = ["celular", "notebook", "zapatillas"]
MAX_ITEMS_PER_TERM = 200
LAYERS = ("bronze", "silver", "gold", "audit")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Params:
    catalog: str
    run_date: date
    sites: tuple[str, ...]
    run_id: str

    def table(self, layer: str, name: str) -> str:
        """Nombre completo de Unity Catalog: catalogo.esquema.tabla"""
        if layer not in LAYERS:
            raise ValueError(f"Capa desconocida: {layer}")
        return f"{self.catalog}.ml_{layer}.{name}"

    def schema(self, layer: str) -> str:
        return f"{self.catalog}.ml_{layer}"

    @property
    def d(self) -> str:
        """Fecha como literal SQL: DATE'2026-11-04'"""
        return f"DATE'{self.run_date.isoformat()}'"

    @property
    def date_key(self) -> int:
        return int(self.run_date.strftime("%Y%m%d"))

    @property
    def sites_sql(self) -> str:
        return ", ".join(f"'{s}'" for s in self.sites)

    @property
    def landing_root(self) -> str:
        return f"/Volumes/{self.catalog}/ml_bronze/landing"


def parse_params(
    catalog: str, run_date: str, sites: str, run_id: str = "", now: datetime | None = None
) -> Params:
    """Valida los parámetros de los widgets / del job.

    Se validan porque terminan dentro de sentencias SQL (identificadores y literales).
    """
    catalog = catalog.strip()
    if not _IDENTIFIER.match(catalog):
        raise ValueError(f"Nombre de catálogo inválido: {catalog!r}")
    now = now or datetime.now(TZ)
    parsed_date = date.fromisoformat(run_date.strip()) if run_date.strip() else now.date()
    site_list = tuple(s.strip().upper() for s in sites.split(",") if s.strip())
    unknown = [s for s in site_list if s not in SITES]
    if not site_list or unknown:
        raise ValueError(f"Sitios inválidos: {unknown or sites!r}. Válidos: {sorted(SITES)}")
    run_id = run_id.strip() or f"dbx__{parsed_date.isoformat()}__{now:%Y%m%dT%H%M%S}"
    return Params(catalog=catalog, run_date=parsed_date, sites=site_list, run_id=run_id)


def get_params(dbutils) -> Params:
    """Crea los widgets (si no existen) y devuelve los parámetros validados.

    En un Job, los parámetros del job con el mismo nombre reemplazan a los widgets.
    """
    defaults = {"catalog": "workspace", "run_date": "", "sites": "MLA,MLB,MLM", "run_id": ""}
    for name, default in defaults.items():
        dbutils.widgets.text(name, default)
    return parse_params(**{name: dbutils.widgets.get(name) for name in defaults})


def split_sql(script: str) -> list[str]:
    """Separa un script SQL en sentencias (no soporta ';' dentro de strings)."""
    lines = [ln for ln in script.splitlines() if not ln.strip().startswith("--")]
    return [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]


def bootstrap_sql(script: str, catalog: str) -> list[str]:
    """Reemplaza {catalog} (sin usar str.format: el SQL tiene llaves en comentarios)."""
    return split_sql(script.replace("{catalog}", catalog))


def log_step(
    spark,
    p: Params,
    step: str,
    *,
    site_id: str | None = None,
    rows_in: int | None = None,
    rows_out: int | None = None,
    details: dict | None = None,
    status: str = "success",
) -> None:
    """Registra una ejecución en ml_audit.pipeline_runs."""
    import json

    from pyspark.sql import functions as F

    row = [
        (
            p.run_id,
            step,
            site_id,
            p.run_date,
            status,
            rows_in,
            rows_out,
            json.dumps(details or {}, default=str, ensure_ascii=False),
        )
    ]
    schema = (
        "run_id string, step string, site_id string, run_date date, status string, "
        "rows_in bigint, rows_out bigint, details string"
    )
    (
        spark.createDataFrame(row, schema)
        .withColumn("logged_at", F.current_timestamp())
        .write.mode("append")
        .saveAsTable(p.table("audit", "pipeline_runs"))
    )
