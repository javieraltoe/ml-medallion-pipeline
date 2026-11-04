"""Test de integridad del DAG: importa sin errores, sin ciclos y con la config pedida."""

from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow", reason="Airflow no instalado (se corre en CI / Docker)")

try:
    from airflow.models import DagBag
except ImportError:  # versiones nuevas que lo movieron
    from airflow.dag_processing.dagbag import DagBag

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_daily_dag_config(dagbag):
    dag = dagbag.get_dag("ml_medallion_daily")
    assert dag is not None
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.default_args["retries"] == 2
    assert dag.default_args["retry_delay"] == timedelta(minutes=5)


def test_layer_order(dagbag):
    dag = dagbag.get_dag("ml_medallion_daily")
    silver = dag.get_task("silver.build_silver")
    assert "bronze.extract_to_bronze" in silver.upstream_task_ids
    assert "bronze.load_fx_rates" in silver.upstream_task_ids
    gold = dag.get_task("gold.build_gold")
    assert "silver.quality_checks_silver" in gold.upstream_task_ids
    assert dag.get_task("silver.quality_checks_silver").retries == 0
