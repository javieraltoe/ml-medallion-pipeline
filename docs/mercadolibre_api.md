# Cómo conectar la API real de Mercado Libre

Desde 2025, Mercado Libre responde `403` a las llamadas sin autenticación, incluida
`/sites/{site_id}/search`. Hace falta una aplicación de desarrollador y un token OAuth 2.0.

> Verificá los pasos contra la documentación oficial
> (developers.mercadolibre.com → *Autenticación y autorización*): Mercado Libre cambia
> permisos y endpoints con frecuencia.

## 1. Crear la aplicación

1. Entrá a **developers.mercadolibre.com.ar** → *Mis aplicaciones* → *Crear aplicación*.
2. Configurá una **Redirect URI** (para uso personal sirve `https://localhost/callback`; el
   código se copia de la barra del navegador).
3. Guardá el **App ID** (`client_id`) y la **Secret Key** (`client_secret`).

## 2. Obtener el primer token (authorization code)

1. Abrí en el navegador (dominio de tu país):

   ```
   https://auth.mercadolibre.com.ar/authorization?response_type=code&client_id=APP_ID&redirect_uri=REDIRECT_URI
   ```

2. Autorizá. Te redirige a `REDIRECT_URI?code=TG-...`. Copiá el `code` (dura pocos minutos).
3. Canjealo:

   ```bash
   curl -X POST https://api.mercadolibre.com/oauth/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -d grant_type=authorization_code -d client_id=APP_ID -d client_secret=SECRET \
     -d code=TG-XXXX -d redirect_uri=REDIRECT_URI
   ```

   La respuesta trae `access_token` (dura ~6 horas) y `refresh_token`.

## 3. Configurar el pipeline

En `.env`:

```
ML_SOURCE_MODE=api
ML_CLIENT_ID=...
ML_CLIENT_SECRET=...
ML_REFRESH_TOKEN=...
```

`make down && make up` para que Airflow tome las variables.

**Cómo maneja el token el pipeline** (`src/ml_pipeline/extract/ml_client.py`):

- Si hay `ML_ACCESS_TOKEN`, lo usa directo (útil para una prueba rápida).
- Si no, renueva con `grant_type=refresh_token`. El refresh token de Mercado Libre es
  **de un solo uso**: el nuevo se guarda en `config/ml_token.json` (ignorado por git) y se
  usa en la próxima corrida.
- Reintenta con backoff exponencial ante `429` y `5xx`, y respeta `Retry-After`.

## Endpoints usados

| Endpoint | Para qué |
|---|---|
| `GET /sites/{site_id}/search?q=&offset=&limit=50` | Publicaciones (Bronze) |
| `GET /currency_conversions/search?from=ARS&to=USD` | Tasa de cambio del día |
| `GET /categories/{category_id}` | Nombre de categorías nuevas (Gold) |

## Seguridad

- `.env` y `config/*.json` están en `.gitignore`. **Nunca** subas credenciales.
- En producción, las credenciales irían en un secret backend de Airflow (Vault, AWS Secrets
  Manager, GCP Secret Manager) en lugar de variables de entorno.
