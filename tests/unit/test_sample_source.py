from datetime import date

from ml_pipeline.extract import sample_source
from ml_pipeline.quality import contracts

DAY = date(2026, 11, 3)


def test_sample_is_deterministic():
    a = list(sample_source.search("MLA", "celular", 100, DAY))
    b = list(sample_source.search("MLA", "celular", 100, DAY))
    assert a == b


def test_sample_respects_contract_and_has_dirty_data():
    items = list(sample_source.search("MLB", "notebook", 200, DAY))
    report = contracts.validate_records(items, contracts.load_contract("ml_search_item"))
    assert report.invalid_records == 0
    ids = [i["id"] for i in items]
    assert len(ids) > len(set(ids)), "debería incluir duplicados para probar la deduplicación"


def test_schema_break_scenario(monkeypatch):
    monkeypatch.setenv("ML_SAMPLE_SCENARIO", "schema_break")
    items = list(sample_source.search("MLA", "celular", 20, DAY))
    assert all("price" not in i for i in items)


def test_fx_rates_are_positive_and_usd_is_one():
    assert sample_source.usd_rate("USD", DAY) == 1.0
    for cur in ("ARS", "BRL", "MXN"):
        assert sample_source.usd_rate(cur, DAY) > 0
