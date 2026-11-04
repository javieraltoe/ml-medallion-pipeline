"""
### Mercado Libre · Pipeline Medallion diario

Extrae publicaciones de Mercado Libre (Argentina, Brasil y México), las aterriza
en **Bronze** tal cual llegan, las limpia/deduplica/normaliza a USD en **Silver**
y construye un modelo estrella + data mart en **Gold**.

| Capa   | Dónde                                        | Patrón de carga            |
|--------|----------------------------------------------|----------------------------|
| Bronze | `data/lake/bronze/...` + `bronze.ml_items_raw` | delete + insert por partición |
| Silver | `silver.item_snapshots`, `silver.items`      | ROW_NUMBER + MERGE         |
| Gold   | `gold.fact_item_daily`, `gold.mart_*`        | delete + insert por fecha  |

* Todas las tareas son **idempotentes**: re-ejecutar o hacer backfill no duplica.
* Los controles de calidad **no reintentan** (un dato malo no se arregla reintentando).
* La lógica vive en `src/ml_pipeline`; el DAG sólo orquesta.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from zoneinfo import ZoneInfo

import pendulum
from airflow.sdk import TaskGroup, dag, get_current_context, task

try:  # Airflow 3
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:  # pragma: no cover
    from airflow.exceptions import AirflowFailException

from ml_pipeline import bronze, db, gold, silver
from ml_pipeline.config import get_settings
from ml_pipeline.quality import checks, contracts

TZ = "America/Argentina/Buenos_Aires"
log = logging.getLogger(__name__)


def _notify_failure(context) -> None:
    """Punto único de alertas. Acá se conectaría Slack / email / PagerDuty."""
    ti = context["task_instance"]
    log.error(
        "ALERTA: falló %s.%s (run %s): %s", ti.dag_id, ti.task_id, context["run_id"], context.get("exception")
    )


def _run_context() -> tuple[date, str]:
    """Fecha del snapshot en hora local y run_id (sirve para manual, scheduled y backfill)."""
    ctx = get_current_context()
    moment = ctx.get("logical_date") or ctx["dag_run"].run_after
    return moment.astimezone(ZoneInfo(TZ)).date(), ctx["run_id"]


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": _notify_failure,
}


@dag(
    dag_id="ml_medallion_daily",
    description="Mercado Libre -> Bronze -> Silver -> Gold con controles de calidad",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["mercadolibre", "medallion", "portfolio"],
    doc_md=__doc__,
)
def ml_medallion_daily():
    sites = get_settings().sites

    @task(retries=1)
    def init_warehouse() -> None:
        db.run_migrations(get_settings().warehouse_dsn)

    with TaskGroup("bronze", tooltip="Datos crudos: data lake + tabla bronze") as bronze_layer:

        @task
        def extract_to_bronze(site_id: str) -> dict:
            run_date, run_id = _run_context()
            try:
                return bronze.extract_to_bronze(site_id, run_date, run_id, get_settings())
            except contracts.SchemaContractError as exc:
                # Cambio de esquema en el origen: reintentar no sirve -> fallar sin retries.
                raise AirflowFailException(str(exc)) from exc

        @task
        def load_fx_rates() -> dict:
            run_date, run_id = _run_context()
            return bronze.load_fx_rates(run_date, run_id, get_settings())

        extract_to_bronze.expand(site_id=sites)
        load_fx_rates()

    with TaskGroup("silver", tooltip="Limpieza, deduplicación, USD y controles") as silver_layer:

        @task
        def build_silver(site_id: str) -> dict:
            run_date, run_id = _run_context()
            return silver.build_silver(site_id, run_date, run_id, get_settings())

        @task(retries=0)
        def quality_checks_silver() -> list[dict]:
            run_date, run_id = _run_context()
            return checks.run_checks("silver", run_date, run_id, get_settings())

        build_silver.expand(site_id=sites) >> quality_checks_silver()

    with TaskGroup("gold", tooltip="Modelo estrella y data marts") as gold_layer:

        @task
        def build_gold() -> dict:
            run_date, run_id = _run_context()
            return gold.build_gold(run_date, run_id, get_settings())

        @task(retries=0)
        def quality_checks_gold() -> list[dict]:
            run_date, run_id = _run_context()
            return checks.run_checks("gold", run_date, run_id, get_settings())

        build_gold() >> quality_checks_gold()

    init_warehouse() >> bronze_layer >> silver_layer >> gold_layer


ml_medallion_daily()
