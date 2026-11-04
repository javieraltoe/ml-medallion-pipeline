"""Configuración centralizada.

Todo se lee de variables de entorno (12-factor) para que el mismo código corra
en la laptop, en Docker/Airflow y en CI sin cambios.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# Sitio de Mercado Libre -> moneda local y país.
SITES: dict[str, dict[str, str]] = {
    "MLA": {"country": "Argentina", "country_code": "AR", "currency": "ARS"},
    "MLB": {"country": "Brasil", "country_code": "BR", "currency": "BRL"},
    "MLM": {"country": "México", "country_code": "MX", "currency": "MXN"},
}


def _env_list(name: str, default: str) -> list[str]:
    return [x.strip() for x in os.getenv(name, default).split(",") if x.strip()]


@dataclass(frozen=True)
class Settings:
    # "sample" = datos sintéticos con la misma forma que la API (sin credenciales)
    # "api"    = API real de Mercado Libre (requiere OAuth)
    source_mode: str = field(default_factory=lambda: os.getenv("ML_SOURCE_MODE", "sample"))
    sites: list[str] = field(default_factory=lambda: _env_list("ML_SITES", "MLA,MLB,MLM"))
    search_terms: list[str] = field(
        default_factory=lambda: _env_list("ML_SEARCH_TERMS", "celular,notebook,zapatillas")
    )
    max_items_per_term: int = field(default_factory=lambda: int(os.getenv("ML_MAX_ITEMS_PER_TERM", "200")))

    # OAuth Mercado Libre
    ml_api_base: str = field(default_factory=lambda: os.getenv("ML_API_BASE", "https://api.mercadolibre.com"))
    ml_client_id: str | None = field(default_factory=lambda: os.getenv("ML_CLIENT_ID"))
    ml_client_secret: str | None = field(default_factory=lambda: os.getenv("ML_CLIENT_SECRET"))
    ml_access_token: str | None = field(default_factory=lambda: os.getenv("ML_ACCESS_TOKEN"))
    ml_refresh_token: str | None = field(default_factory=lambda: os.getenv("ML_REFRESH_TOKEN"))
    ml_token_cache: Path = field(
        default_factory=lambda: Path(os.getenv("ML_TOKEN_CACHE", "/opt/airflow/config/ml_token.json"))
    )

    # Data lake (Bronze en archivos) y warehouse (Postgres)
    lake_root: Path = field(default_factory=lambda: Path(os.getenv("LAKE_ROOT", "data/lake")))
    warehouse_dsn: str = field(
        default_factory=lambda: os.getenv(
            "WAREHOUSE_DSN", "postgresql://warehouse:warehouse@localhost:5433/warehouse"
        )
    )

    # Umbrales de calidad
    volume_drop_threshold: float = field(
        default_factory=lambda: float(os.getenv("DQ_VOLUME_DROP_THRESHOLD", "0.5"))
    )
    max_quarantine_ratio: float = field(
        default_factory=lambda: float(os.getenv("DQ_MAX_QUARANTINE_RATIO", "0.10"))
    )


def get_settings() -> Settings:
    return Settings()
