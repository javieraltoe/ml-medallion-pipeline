"""Cliente de la API de Mercado Libre.

Desde 2025 Mercado Libre exige un access token OAuth 2.0 en todos los endpoints
(incluido /sites/{site}/search). Este cliente:

* usa ML_ACCESS_TOKEN si está definido, o
* renueva el token con el flujo refresh_token. Los refresh tokens de ML son de
  un solo uso: el nuevo se persiste en ML_TOKEN_CACHE para la próxima corrida.
* reintenta con backoff exponencial ante 429 / 5xx y respeta la paginación.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ml_pipeline.config import Settings

log = logging.getLogger(__name__)

PAGE_SIZE = 50  # máximo permitido por /search


class MercadoLibreAuthError(RuntimeError):
    pass


class MercadoLibreClient:
    def __init__(self, settings: Settings, session: requests.Session | None = None):
        self.s = settings
        self.session = session or self._build_session()
        self._token: str | None = settings.ml_access_token
        self._token_expires_at: float = float("inf") if self._token else 0.0

    # ------------------------------------------------------------------ HTTP
    @staticmethod
    def _build_session() -> requests.Session:
        retry = Retry(
            total=5,
            backoff_factor=1.5,  # 0s, 1.5s, 3s, 6s, 12s
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
            respect_retry_after_header=True,
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers.update({"Accept": "application/json", "User-Agent": "ml-medallion-pipeline/1.0"})
        return session

    # ----------------------------------------------------------------- OAuth
    def _load_cached_refresh_token(self) -> str | None:
        cache: Path = self.s.ml_token_cache
        if cache.exists():
            return json.loads(cache.read_text()).get("refresh_token")
        return self.s.ml_refresh_token

    def _refresh_access_token(self) -> None:
        refresh_token = self._load_cached_refresh_token()
        if not (self.s.ml_client_id and self.s.ml_client_secret and refresh_token):
            raise MercadoLibreAuthError(
                "Modo api sin credenciales: definí ML_ACCESS_TOKEN, o bien ML_CLIENT_ID + "
                "ML_CLIENT_SECRET + ML_REFRESH_TOKEN (ver docs/mercadolibre_api.md)."
            )
        resp = self.session.post(
            f"{self.s.ml_api_base}/oauth/token",
            data={
                "grant_type": "refresh_token",
                "client_id": self.s.ml_client_id,
                "client_secret": self.s.ml_client_secret,
                "refresh_token": refresh_token,
            },
            timeout=30,
        )
        if resp.status_code != 200:
            raise MercadoLibreAuthError(f"No se pudo renovar el token: {resp.status_code} {resp.text[:200]}")
        payload = resp.json()
        self._token = payload["access_token"]
        self._token_expires_at = time.time() + int(payload.get("expires_in", 21600)) - 60
        # El refresh token rota: guardamos el nuevo.
        self.s.ml_token_cache.parent.mkdir(parents=True, exist_ok=True)
        self.s.ml_token_cache.write_text(json.dumps({"refresh_token": payload["refresh_token"]}))
        log.info("Access token renovado; vence en %ss", payload.get("expires_in"))

    def _auth_header(self) -> dict[str, str]:
        if not self._token or time.time() >= self._token_expires_at:
            self._refresh_access_token()
        return {"Authorization": f"Bearer {self._token}"}

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.s.ml_api_base}{path}"
        resp = self.session.get(url, params=params, headers=self._auth_header(), timeout=30)
        if resp.status_code == 401:  # token vencido antes de lo esperado
            self._token = None
            resp = self.session.get(url, params=params, headers=self._auth_header(), timeout=30)
        if resp.status_code == 403:
            raise MercadoLibreAuthError(f"403 en {path}: la app no tiene permiso para este recurso")
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------ Endpoints
    def search(self, site_id: str, query: str, max_items: int) -> Iterator[dict[str, Any]]:
        """Pagina /sites/{site_id}/search y devuelve cada resultado crudo."""
        offset = 0
        while offset < max_items:
            limit = min(PAGE_SIZE, max_items - offset)
            data = self.get(f"/sites/{site_id}/search", {"q": query, "offset": offset, "limit": limit})
            results = data.get("results", [])
            if not results:
                break
            yield from results
            offset += len(results)
            total = data.get("paging", {}).get("total", 0)
            if offset >= total:
                break

    def usd_rate(self, currency_id: str) -> float:
        """USD por 1 unidad de la moneda (endpoint /currency_conversions/search)."""
        if currency_id == "USD":
            return 1.0
        data = self.get("/currency_conversions/search", {"from": currency_id, "to": "USD"})
        return float(data["ratio"])

    def category(self, category_id: str) -> dict[str, Any]:
        return self.get(f"/categories/{category_id}")
