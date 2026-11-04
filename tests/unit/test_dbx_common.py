"""Tests de las utilidades de los notebooks de Databricks (no requieren Spark)."""

import sys
from datetime import date, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "databricks_lakehouse"))

import dbx_common as dc  # noqa: E402

DDL = Path(__file__).resolve().parents[2] / "databricks_lakehouse" / "sql" / "ddl.sql"


def test_defaults_use_today_in_argentina():
    now = datetime(2026, 11, 4, 1, 30, tzinfo=dc.TZ)
    p = dc.parse_params("workspace", "", "MLA, mlb", now=now)
    assert p.run_date == date(2026, 11, 4)
    assert p.sites == ("MLA", "MLB")
    assert p.run_id.startswith("dbx__2026-11-04")


def test_sql_helpers():
    p = dc.parse_params("workspace", "2026-11-04", "MLA,MLM", run_id="r1")
    assert p.table("gold", "dim_seller") == "workspace.ml_gold.dim_seller"
    assert p.d == "DATE'2026-11-04'"
    assert p.date_key == 20261104
    assert p.sites_sql == "'MLA', 'MLM'"
    assert p.landing_root == "/Volumes/workspace/ml_bronze/landing"


@pytest.mark.parametrize(
    "catalog, run_date, sites",
    [
        ("bad name", "", "MLA"),
        ("x;DROP", "", "MLA"),
        ("ok", "2026-13-01", "MLA"),
        ("ok", "", "MLC"),
        ("ok", "", ""),
    ],
)
def test_invalid_params_are_rejected(catalog, run_date, sites):
    with pytest.raises(ValueError):
        dc.parse_params(catalog, run_date, sites)


def test_ddl_bootstrap_keeps_braces_in_comments_and_splits_statements():
    stmts = dc.bootstrap_sql(DDL.read_text(encoding="utf-8"), "mi_catalogo")
    assert all("{catalog}" not in s for s in stmts)
    assert any("/sites/{site}/search" in s for s in stmts)  # llaves literales intactas
    assert sum(s.startswith("CREATE SCHEMA") for s in stmts) == 4
    assert any("GENERATED ALWAYS AS IDENTITY" in s for s in stmts)
