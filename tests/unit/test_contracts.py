import pytest

from ml_pipeline.quality import contracts

CONTRACT = contracts.load_contract("ml_search_item")


def _item(**overrides):
    item = {
        "id": "MLA1",
        "site_id": "MLA",
        "title": "x",
        "price": 10.5,
        "currency_id": "ARS",
        "category_id": "MLA1055",
        "available_quantity": 1,
        "seller": {"id": 1},
    }
    item.update(overrides)
    return item


def test_valid_records_pass():
    report = contracts.validate_records([_item(), _item(price=None)], CONTRACT)
    assert report.invalid_records == 0
    contracts.enforce(report, CONTRACT["max_violation_ratio"])


def test_missing_required_field_is_a_violation():
    rec = _item()
    del rec["price"]
    report = contracts.validate_records([rec], CONTRACT)
    assert report.violations == {"price: ausente": 1}


def test_wrong_type_is_a_violation_and_bool_is_not_integer():
    report = contracts.validate_records([_item(available_quantity=True), _item(price="10")], CONTRACT)
    assert report.invalid_records == 2


def test_new_fields_are_reported_as_drift_but_do_not_fail():
    report = contracts.validate_records([_item(promotion_tag="DEAL")], CONTRACT)
    assert report.unknown_fields == {"promotion_tag"}
    contracts.enforce(report, 0.0)


def test_enforce_raises_over_threshold():
    broken = [_item() for _ in range(9)] + [{"id": "MLA9"}]
    report = contracts.validate_records(broken, CONTRACT)
    with pytest.raises(contracts.SchemaContractError):
        contracts.enforce(report, 0.05)
