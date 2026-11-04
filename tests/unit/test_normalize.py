import json

import pandas as pd
import pytest

from ml_pipeline.transform import normalize

RATES = {"ARS": 0.0007, "BRL": 0.20, "USD": 1.0}


def _row(seq, **overrides):
    item = {
        "id": f"MLA{seq}",
        "site_id": "MLA",
        "title": "Celular X",
        "category_id": "MLA1055",
        "price": 150000,
        "original_price": None,
        "currency_id": "ARS",
        "available_quantity": 5,
        "seller": {"id": 10, "nickname": "VENDEDOR"},
        "shipping": {"free_shipping": True},
        "attributes": [{"id": "BRAND", "value_name": "Samsung"}],
    }
    item.update(overrides)
    return (seq, "celular", json.dumps(item))


def test_flatten_extracts_nested_fields():
    df = normalize.flatten_items([_row(1)])
    assert df.loc[0, "seller_id"] == 10
    assert df.loc[0, "brand"] == "Samsung"
    assert bool(df.loc[0, "free_shipping"]) is True


@pytest.mark.parametrize(
    "overrides, reason",
    [
        ({"price": None}, "precio_nulo"),
        ({"price": 0}, "precio_no_positivo"),
        ({"price": -5}, "precio_no_positivo"),
        ({"currency_id": "EUR"}, "moneda_sin_tipo_de_cambio"),
        ({"seller": {}}, "vendedor_ausente"),
        ({"id": None}, "id_ausente"),
    ],
)
def test_business_rules_send_bad_rows_to_quarantine(overrides, reason):
    df = normalize.flatten_items([_row(1), _row(2, **overrides)])
    valid, rejected = normalize.split_valid_rejected(df, set(RATES))
    assert len(valid) == 1
    assert rejected["reason"].tolist() == [reason]


def test_add_usd_amount_keeps_original_currency_and_rate():
    df = pd.DataFrame(
        {
            "currency_id": ["ARS", "BRL", "USD"],
            "price": [150000.0, 1000.0, 250.0],
            "original_price": [200000.0, None, 200.0],
        }
    )
    out = normalize.add_usd_amount(df, RATES)
    assert out["price_usd"].tolist() == [105.0, 200.0, 250.0]
    assert out["currency_id"].tolist() == ["ARS", "BRL", "USD"]  # no se pierde la moneda original
    assert out["usd_per_unit"].tolist() == [0.0007, 0.20, 1.0]  # trazabilidad de la tasa
    assert out.loc[0, "discount_pct"] == 25.0
    assert pd.isna(out.loc[2, "discount_pct"])  # original < precio => no es descuento


def test_add_usd_amount_fails_on_unknown_currency():
    df = pd.DataFrame({"currency_id": ["ARS", "XYZ"], "price": [1.0, 1.0], "original_price": [None, None]})
    with pytest.raises(normalize.UnknownCurrencyError, match="XYZ"):
        normalize.add_usd_amount(df, RATES)
