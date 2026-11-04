"""Transformaciones puras con pandas (sin I/O => fáciles de testear)."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

SNAPSHOT_COLUMNS = [
    "item_id",
    "snapshot_date",
    "site_id",
    "search_term",
    "title",
    "category_id",
    "seller_id",
    "condition",
    "listing_type_id",
    "brand",
    "currency_id",
    "price",
    "original_price",
    "usd_per_unit",
    "price_usd",
    "discount_pct",
    "available_quantity",
    "free_shipping",
    "logistic_type",
    "official_store_id",
    "permalink",
]


class UnknownCurrencyError(ValueError):
    pass


def _brand(attributes: Any) -> str | None:
    if isinstance(attributes, list):
        for attr in attributes:
            if isinstance(attr, dict) and attr.get("id") == "BRAND":
                return attr.get("value_name")
    return None


def flatten_items(rows: list[tuple[int, str, str | dict]]) -> pd.DataFrame:
    """Convierte (record_seq, search_term, payload_json) en un DataFrame tabular y tipado."""
    records = []
    for seq, term, payload in rows:
        item = json.loads(payload) if isinstance(payload, str) else payload
        seller = item.get("seller") or {}
        shipping = item.get("shipping") or {}
        records.append(
            {
                "record_seq": seq,
                "search_term": term,
                "item_id": item.get("id"),
                "site_id": item.get("site_id"),
                "title": (item.get("title") or "").strip() or None,
                "category_id": item.get("category_id"),
                "seller_id": seller.get("id"),
                "seller_nickname": seller.get("nickname"),
                "condition": item.get("condition"),
                "listing_type_id": item.get("listing_type_id"),
                "brand": _brand(item.get("attributes")),
                "currency_id": (item.get("currency_id") or "").upper() or None,
                "price": item.get("price"),
                "original_price": item.get("original_price"),
                "available_quantity": item.get("available_quantity"),
                "free_shipping": shipping.get("free_shipping"),
                "logistic_type": shipping.get("logistic_type"),
                "official_store_id": item.get("official_store_id"),
                "permalink": item.get("permalink"),
                "raw_payload": payload if isinstance(payload, str) else json.dumps(payload),
            }
        )
    df = pd.DataFrame.from_records(records)
    if df.empty:
        return df
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["original_price"] = pd.to_numeric(df["original_price"], errors="coerce")
    df["seller_id"] = pd.to_numeric(df["seller_id"], errors="coerce").astype("Int64")
    df["official_store_id"] = pd.to_numeric(df["official_store_id"], errors="coerce").astype("Int64")
    df["available_quantity"] = pd.to_numeric(df["available_quantity"], errors="coerce").astype("Int64")
    return df


def split_valid_rejected(df: pd.DataFrame, known_currencies: set[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reglas de negocio. Lo que no cumple va a cuarentena con el motivo (no se descarta en silencio)."""
    if df.empty:
        return df, df.assign(reason=pd.Series(dtype="string"))
    rules: list[tuple[str, pd.Series]] = [
        ("id_ausente", df["item_id"].isna()),
        ("titulo_ausente", df["title"].isna()),
        ("vendedor_ausente", df["seller_id"].isna()),
        ("categoria_ausente", df["category_id"].isna()),
        ("precio_nulo", df["price"].isna()),
        ("precio_no_positivo", df["price"].notna() & (df["price"] <= 0)),
        ("moneda_sin_tipo_de_cambio", ~df["currency_id"].isin(known_currencies)),
    ]
    reason = pd.Series(pd.NA, index=df.index, dtype="string")
    for name, mask in rules:  # el primer motivo que aplica gana
        reason = reason.mask(reason.isna() & mask, name)
    rejected = df[reason.notna()].assign(reason=reason[reason.notna()])
    valid = df[reason.isna()].copy()
    return valid, rejected


def add_usd_amount(df: pd.DataFrame, usd_per_unit: dict[str, float]) -> pd.DataFrame:
    """Normaliza importes a USD conservando la moneda original y la tasa aplicada.

    Equivalente al ejercicio 2 del simulacro, con tasas fechadas reales del pipeline.
    """
    out = df.copy()
    out["usd_per_unit"] = out["currency_id"].map(usd_per_unit)
    unknown = sorted(out.loc[out["usd_per_unit"].isna(), "currency_id"].dropna().unique())
    if unknown or out["usd_per_unit"].isna().any():
        raise UnknownCurrencyError(f"Monedas sin tipo de cambio: {unknown or ['<nulo>']}")
    out["price_usd"] = (out["price"] * out["usd_per_unit"]).round(2)
    has_discount = out["original_price"].notna() & (out["original_price"] > out["price"])
    out["discount_pct"] = (
        (100 * (out["original_price"] - out["price"]) / out["original_price"]).where(has_discount).round(2)
    )
    return out
