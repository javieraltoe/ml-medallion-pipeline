# Mapeo source-to-target

Origen: `GET /sites/{site_id}/search` (cada elemento de `results[]`).
Es el mismo artefacto que se pide en una migración entre sistemas: qué campo va a dónde,
con qué tipo y con qué regla.

## API → Silver (`silver.item_snapshots`)

| Campo origen | Tipo origen | Columna destino | Tipo destino | Regla de transformación |
|---|---|---|---|---|
| `id` | string | `item_id` | text (PK) | Requerido. Nulo → cuarentena `id_ausente` |
| *(fecha de la corrida)* | — | `snapshot_date` | date (PK) | `logical_date` del DAG en hora de Argentina |
| `site_id` | string | `site_id` | text | Requerido |
| *(parámetro `q`)* | — | `search_term` | text | Término que trajo el resultado |
| `title` | string | `title` | text | `trim`; vacío → cuarentena `titulo_ausente` |
| `category_id` | string | `category_id` | text | Requerido |
| `seller.id` | int | `seller_id` | bigint | Nulo → cuarentena `vendedor_ausente` |
| `seller.nickname` | string | `silver.sellers.nickname` | text | Último valor visto |
| `condition` | string | `condition` | text | `new` / `used` |
| `listing_type_id` | string | `listing_type_id` | text | — |
| `attributes[id=BRAND].value_name` | string | `brand` | text | Se busca el atributo `BRAND` |
| `currency_id` | string | `currency_id` | text | `upper()`. Se **conserva** la moneda original |
| `price` | number | `price` | numeric(18,2) | Nulo → `precio_nulo`; ≤ 0 → `precio_no_positivo` |
| `original_price` | number | `original_price` | numeric(18,2) | Opcional |
| *(silver.fx_rates)* | — | `usd_per_unit` | numeric(20,10) | Tasa más reciente con `rate_date ≤ snapshot_date` |
| — | — | `price_usd` | numeric(18,2) | `round(price * usd_per_unit, 2)`. Moneda sin tasa → `moneda_sin_tipo_de_cambio` |
| — | — | `discount_pct` | numeric(6,2) | `100*(original-price)/original` sólo si `original > price` |
| `available_quantity` | int | `available_quantity` | integer | — |
| `shipping.free_shipping` | bool | `free_shipping` | boolean | — |
| `shipping.logistic_type` | string | `logistic_type` | text | — |
| `official_store_id` | int/null | `official_store_id` | bigint | — |
| `permalink` | string | `permalink` | text | — |
| *(campos no mapeados)* | — | — | — | Quedan en `bronze.payload` (JSONB); si son nuevos, el contrato avisa |

**Deduplicación:** clave de negocio `(item_id, snapshot_date)`. Si un id llega varias veces
en el mismo día se conserva el último recibido (`record_seq` mayor).

## Silver → Gold (`gold.fact_item_daily`)

| Origen | Destino | Regla |
|---|---|---|
| `snapshot_date` | `date_key` | `YYYYMMDD` entero → `dim_date` |
| `site_id` | `site_id` | FK a `dim_site` |
| `category_id` | `category_id` | FK a `dim_category`. Id no conocido → “Sin clasificar (id)”; en modo api se completa con `/categories/{id}` |
| `seller_id` | `seller_id` | FK a `dim_seller` |
| `price` | `price_local` | — |
| `price_usd` | `price_usd` | — |
| `condition` | `is_new` | `condition = 'new'` |
| resto | igual nombre | Copia directa |

**Reconciliación obligatoria:** mismas filas y misma suma de `price_usd` entre Silver y Gold
por fecha (controles `gold_fact_matches_silver_rows` y `gold_fact_matches_silver_amounts`).
