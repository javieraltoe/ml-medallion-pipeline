"""Fuente sintética con la MISMA forma que /sites/{site}/search.

Sirve para correr el pipeline sin credenciales (demo, CI, entrevistas).
Es determinística: misma fecha + sitio => mismos datos, así las re-ejecuciones
y backfills son reproducibles (clave para demostrar idempotencia).

Inyecta "suciedad" realista a propósito para que los controles de calidad
tengan algo que detectar:
  * duplicados por solapamiento de paginación
  * precios nulos o en cero
  * publicaciones en USD dentro de MLA (pasa en la vida real)
  * un campo nuevo que aparece a partir de cierta fecha (schema drift)

Escenarios de demo (variable ML_SAMPLE_SCENARIO):
  * volume_drop   -> devuelve ~20% de las filas (dispara el control de volumen)
  * schema_break  -> elimina el campo "price" (rompe el contrato)
"""

from __future__ import annotations

import hashlib
import os
import random
from collections.abc import Iterator
from datetime import date
from typing import Any

from ml_pipeline.config import SITES

# Tasas ilustrativas (USD por unidad). Son ficticias: sólo para la demo.
BASE_USD_RATE = {"ARS": 0.00072, "BRL": 0.185, "MXN": 0.054, "USD": 1.0}

TERMS = {
    "celular": {
        "usd": 320,
        "categories": [("1055", 0.85), ("3813", 0.15)],
        "brands": ["Samsung", "Motorola", "Xiaomi", "Apple"],
    },
    "notebook": {
        "usd": 850,
        "categories": [("1652", 1.0)],
        "brands": ["Lenovo", "HP", "Asus", "Dell", "Apple"],
    },
    "zapatillas": {
        "usd": 95,
        "categories": [("109027", 1.0)],
        "brands": ["Nike", "Adidas", "Topper", "Puma"],
    },
}
POOL_SIZE = 220  # publicaciones "existentes" por sitio y término
SELLERS_PER_SITE = 45
SCHEMA_DRIFT_FROM = date(2026, 10, 25)
SITE_SELLER_OFFSET = {"MLA": 100_000, "MLB": 200_000, "MLM": 300_000}


def _rng(*parts: object) -> random.Random:
    seed = int(hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()[:16], 16)
    return random.Random(seed)


def usd_rate(currency_id: str, on: date) -> float:
    """Tasa sintética con una deriva diaria pequeña y determinística."""
    base = BASE_USD_RATE[currency_id]
    if currency_id == "USD":
        return 1.0
    drift = _rng("fx", currency_id, on.isoformat()).uniform(-0.01, 0.01)
    days = (on - date(2026, 1, 1)).days
    devaluation = 1 - 0.0004 * days if currency_id == "ARS" else 1.0
    return round(base * devaluation * (1 + drift), 8)


# Cambios en los vendedores a lo largo del tiempo (para la dimensión SCD tipo 2):
#   * 1 de cada 9 vendedores es tienda oficial desde siempre
#   * otro 1 de cada 9 se convierte en tienda oficial a partir de OFFICIAL_FROM
#   * 1 de cada 11 cambia su nombre visible a partir de RENAME_FROM
OFFICIAL_FROM = date(2026, 10, 28)
RENAME_FROM = date(2026, 10, 30)


def _is_official_store(seller_n: int, snapshot: date) -> bool:
    return seller_n % 9 == 0 or (seller_n % 9 == 1 and snapshot >= OFFICIAL_FROM)


def _seller_nickname(site_id: str, seller_n: int, snapshot: date) -> str:
    if seller_n % 11 == 3 and snapshot >= RENAME_FROM:
        return f"TIENDA_{site_id}_{seller_n:03d}"
    return f"VENDEDOR_{site_id}_{seller_n:03d}"


def _item(site_id: str, term: str, n: int, snapshot: date) -> dict[str, Any] | None:
    meta = TERMS[term]
    stable = _rng(site_id, term, n)  # atributos que no cambian
    daily = _rng(site_id, term, n, snapshot)  # atributos del día

    # ~8% de las publicaciones no están activas en un día dado
    if daily.random() < 0.08:
        return None

    currency = SITES[site_id]["currency"]
    if site_id == "MLA" and term == "notebook" and stable.random() < 0.12:
        currency = "USD"

    cat_suffix = stable.choices([c for c, _ in meta["categories"]], [w for _, w in meta["categories"]])[0]
    usd_price = meta["usd"] * stable.uniform(0.35, 2.2) * (1 + daily.uniform(-0.04, 0.04))
    if cat_suffix == "3813":  # accesorios: mucho más baratos
        usd_price *= 0.08
    price: float | None = round(usd_price / usd_rate(currency, snapshot), 2)
    roll = daily.random()
    if roll < 0.01:
        price = None
    elif roll < 0.015:
        price = 0.0

    seller_n = stable.randrange(SELLERS_PER_SITE)
    brand = stable.choice(meta["brands"])
    item_num = (
        1_000_000_000 + int(hashlib.md5(f"{site_id}{term}{n}".encode()).hexdigest()[:8], 16) % 900_000_000
    )
    item_id = f"{site_id}{item_num}"

    item: dict[str, Any] = {
        "id": item_id,
        "site_id": site_id,
        "title": f"{term.title()} {brand} Modelo {stable.randint(10, 99)}{stable.choice('XSPM')}",
        "condition": "new" if stable.random() < 0.85 else "used",
        "category_id": f"{site_id}{cat_suffix}",
        "price": price,
        "original_price": round(price * daily.uniform(1.05, 1.45), 2)
        if price and daily.random() < 0.25
        else None,
        "currency_id": currency,
        "available_quantity": daily.choice([1, 5, 25, 50, 100, 250, 500]),
        "listing_type_id": stable.choice(["gold_special", "gold_pro"]),
        "permalink": f"https://articulo.mercadolibre.example/{item_id}",
        "thumbnail": f"https://http2.mlstatic.example/{item_id}-I.jpg",
        "accepts_mercadopago": True,
        "official_store_id": (1000 + seller_n) if _is_official_store(seller_n, snapshot) else None,
        "seller": {
            # Los ids de vendedor son únicos por sitio (cada país tiene su propio rango).
            "id": SITE_SELLER_OFFSET[site_id] + seller_n * 7919 % 99_991,
            "nickname": _seller_nickname(site_id, seller_n, snapshot),
        },
        "shipping": {
            "free_shipping": stable.random() < 0.6,
            "logistic_type": stable.choice(["fulfillment", "cross_docking", "drop_off"]),
        },
        "attributes": [
            {"id": "BRAND", "name": "Marca", "value_name": brand},
            {"id": "ITEM_CONDITION", "name": "Condición del ítem", "value_name": "Nuevo"},
        ],
    }
    if snapshot >= SCHEMA_DRIFT_FROM:
        item["promotion_tag"] = daily.choice([None, "DEAL_OF_THE_DAY", "LIGHTNING"])  # campo nuevo
    return item


def search(site_id: str, query: str, max_items: int, snapshot: date) -> Iterator[dict[str, Any]]:
    scenario = os.getenv("ML_SAMPLE_SCENARIO", "")
    rng = _rng("page", site_id, query, snapshot)
    # El pool crece con el tiempo: aparecen publicaciones nuevas.
    pool = POOL_SIZE + (snapshot - date(2026, 1, 1)).days // 3
    emitted = 0
    for n in range(pool):
        if emitted >= max_items:
            break
        item = _item(site_id, query, n, snapshot)
        if item is None:
            continue
        if scenario == "volume_drop" and rng.random() > 0.2:
            continue
        if scenario == "schema_break":
            item.pop("price", None)
        yield item
        emitted += 1
        # ~3% de duplicados por solapamiento de páginas
        if rng.random() < 0.03:
            yield dict(item)
