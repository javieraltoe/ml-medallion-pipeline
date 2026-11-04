"""Test end-to-end contra un Postgres real (en CI se levanta como service container).

WAREHOUSE_DSN=postgresql://warehouse:warehouse@localhost:5433/warehouse pytest -m integration
"""

import os
from datetime import date, timedelta

import pytest

pytestmark = pytest.mark.integration

if not os.getenv("WAREHOUSE_DSN"):
    pytest.skip("WAREHOUSE_DSN no definido", allow_module_level=True)

from ml_pipeline import __main__ as cli  # noqa: E402
from ml_pipeline import db  # noqa: E402
from ml_pipeline.config import get_settings  # noqa: E402
from ml_pipeline.quality import checks, contracts  # noqa: E402

START = date(2026, 10, 1)
DAYS = [START + timedelta(days=i) for i in range(8)]


def _counts():
    sql = """SELECT (SELECT count(*) FROM bronze.ml_items_raw),
                    (SELECT count(*) FROM silver.item_snapshots),
                    (SELECT count(*) FROM gold.fact_item_daily),
                    (SELECT coalesce(sum(price_usd), 0) FROM gold.fact_item_daily)"""
    with db.connect(get_settings().warehouse_dsn) as conn:
        return db.fetch_one(conn, sql)


@pytest.fixture(scope="module", autouse=True)
def clean_warehouse(tmp_path_factory):
    os.environ["LAKE_ROOT"] = str(tmp_path_factory.mktemp("lake"))
    with db.connect(get_settings().warehouse_dsn) as conn:
        db.execute(conn, "DROP SCHEMA IF EXISTS bronze, silver, gold, audit CASCADE")
    for day in DAYS:
        cli.run_day(day)


def test_backfill_loads_every_layer():
    bronze_rows, silver_rows, gold_rows, _ = _counts()
    assert silver_rows == gold_rows > 0
    assert bronze_rows > silver_rows  # duplicados y rechazados quedan sólo en bronze


def test_rerun_is_idempotent():
    before = _counts()
    cli.run_day(DAYS[3])
    cli.run_day(DAYS[-1])
    assert _counts() == before


def test_volume_drop_is_detected(monkeypatch):
    monkeypatch.setenv("ML_SAMPLE_SCENARIO", "volume_drop")
    with pytest.raises(checks.DataQualityError, match="silver_volume_vs_7d_avg"):
        cli.run_day(DAYS[-1] + timedelta(days=1))


def test_schema_change_stops_the_pipeline(monkeypatch):
    monkeypatch.setenv("ML_SAMPLE_SCENARIO", "schema_break")
    with pytest.raises(contracts.SchemaContractError):
        cli.run_day(DAYS[-1] + timedelta(days=2))
