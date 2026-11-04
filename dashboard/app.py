"""Dashboard de la capa Gold.

streamlit run dashboard/app.py      (o el servicio "dashboard" de docker compose)
"""

from __future__ import annotations

import os
from datetime import timedelta

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st

import queries as q

st.set_page_config(page_title="Mercado Libre · Precios en USD", page_icon="📊", layout="wide")

DSN = os.getenv("WAREHOUSE_DSN", "postgresql://warehouse:warehouse@localhost:5433/warehouse")
COUNTRY_COLORS = alt.Scale(domain=["Argentina", "Brasil", "México"], range=["#4C78A8", "#54A24B", "#E45756"])


@st.cache_resource
def get_conn():
    conn = psycopg2.connect(DSN)
    conn.autocommit = True  # sólo lectura: cada consulta ve los datos más recientes
    return conn


@st.cache_data(ttl=300, show_spinner=False)
def load(name: str, *args):
    return getattr(q, name)(get_conn(), *args)


# ------------------------------------------------------------------ Filtros
try:
    dates = load("available_dates")
except psycopg2.Error as exc:
    st.error(f"No se pudo conectar al warehouse ({exc}). ¿Está corriendo `docker compose up`?")
    st.stop()

if not dates:
    st.title("📊 Mercado Libre · Precios en USD")
    st.info("Todavía no hay datos en Gold. Ejecutá el DAG `ml_medallion_daily` en Airflow y recargá.")
    st.stop()

with st.sidebar:
    st.header("Filtros")
    day = st.selectbox("Día", dates, format_func=lambda d: d.strftime("%d/%m/%Y"))
    window = st.slider("Días de historia para las tendencias", 3, 60, 14)
    countries = st.multiselect(
        "Países", ["Argentina", "Brasil", "México"], default=["Argentina", "Brasil", "México"]
    )
    if st.button("🔄 Recargar datos"):
        st.cache_data.clear()
    st.caption("Fuente: capa **Gold** del warehouse (`gold.mart_category_daily`, `gold.fact_item_daily`).")

if not countries:
    st.warning("Elegí al menos un país.")
    st.stop()

site_by_country = {"Argentina": "MLA", "Brasil": "MLB", "México": "MLM"}
sites = [site_by_country[c] for c in countries]
start = day - timedelta(days=window - 1)

mart = load("mart", start, day)
mart = mart[mart["country"].isin(countries)]
today = mart[mart["full_date"] == day]

# ------------------------------------------------------------------ Encabezado + KPIs
st.title("📊 Mercado Libre · ¿Cuánto cuesta lo mismo en cada país?")
st.caption(
    f"Snapshot del {day:%d/%m/%Y} · precios convertidos a USD con la tasa del día · {', '.join(countries)}"
)

k = load("kpis", day, sites)
cur = k.iloc[0] if len(k) else None
prev = k.iloc[1] if len(k) > 1 else None


def delta(col: str, fmt: str = "{:+,.0f}"):
    if cur is None or prev is None or pd.isna(prev[col]):
        return None
    return fmt.format(cur[col] - prev[col])


c1, c2, c3, c4 = st.columns(4)
c1.metric("Publicaciones", f"{int(cur['items']):,}" if cur is not None else "-", delta("items"))
c2.metric("Vendedores", f"{int(cur['sellers']):,}" if cur is not None else "-", delta("sellers"))
c3.metric(
    "Precio mediano (USD)",
    f"US$ {cur['median_price_usd']:,.2f}" if cur is not None else "-",
    delta("median_price_usd", "{:+,.2f}"),
)
c4.metric(
    "Con envío gratis",
    f"{cur['pct_free_shipping']:.1f}%" if cur is not None else "-",
    delta("pct_free_shipping", "{:+.1f} pp"),
)

tab_prices, tab_trend, tab_detail, tab_quality = st.tabs(
    ["💲 Comparación de precios", "📈 Tendencias", "🔎 Detalle", "✅ Calidad de datos"]
)

# ------------------------------------------------------------------ Comparación
with tab_prices:
    st.subheader("Precio mediano en USD por categoría y país")
    st.caption(
        "La barra es la mediana; la línea negra va del percentil 25 al 75 (rango típico de precios). "
        "Cada categoría tiene su propia escala para que se puedan comparar los países dentro de ella."
    )
    groups_today = sorted(today["category_group"].unique())
    if not groups_today:
        st.info("No hay datos para este día con los filtros elegidos.")
    for col, group in zip(st.columns(max(len(groups_today), 1)), groups_today, strict=False):
        data = today[today["category_group"] == group]
        base = alt.Chart(data).encode(x=alt.X("country:N", title=None, axis=alt.Axis(labelAngle=0)))
        bars = base.mark_bar().encode(
            y=alt.Y("median_price_usd:Q", title="USD"),
            color=alt.Color("country:N", scale=COUNTRY_COLORS, legend=None),
            tooltip=[
                alt.Tooltip("country:N", title="País"),
                alt.Tooltip("category_name:N", title="Categoría"),
                alt.Tooltip("median_price_usd:Q", title="Mediana USD", format=",.2f"),
                alt.Tooltip("p25_price_usd:Q", title="P25 USD", format=",.2f"),
                alt.Tooltip("p75_price_usd:Q", title="P75 USD", format=",.2f"),
                alt.Tooltip("items:Q", title="Publicaciones"),
            ],
        )
        whiskers = base.mark_rule(color="black", opacity=0.6, strokeWidth=2).encode(
            y="p25_price_usd:Q", y2="p75_price_usd:Q"
        )
        chart = (bars + whiskers).properties(height=320, title=alt.TitleParams(group, anchor="middle"))
        col.altair_chart(chart, use_container_width=True)

    left, right = st.columns(2)
    with left:
        st.subheader("% con envío gratis")
        heat = (
            alt.Chart(today)
            .mark_rect()
            .encode(
                x=alt.X("country:N", title=None, axis=alt.Axis(labelAngle=0, orient="top")),
                y=alt.Y("category_group:N", title=None, axis=alt.Axis(labelLimit=250)),
                color=alt.Color("pct_free_shipping:Q", title="%", scale=alt.Scale(scheme="greens")),
                tooltip=["country", "category_name", alt.Tooltip("pct_free_shipping:Q", format=".1f")],
            )
        )
        labels = heat.mark_text(baseline="middle").encode(
            text=alt.Text("pct_free_shipping:Q", format=".0f"), color=alt.value("black")
        )
        st.altair_chart((heat + labels).properties(height=300), use_container_width=True)
    with right:
        st.subheader("Descuento promedio (%)")
        disc = (
            alt.Chart(today)
            .mark_bar()
            .encode(
                y=alt.Y("category_group:N", title=None, axis=alt.Axis(labelLimit=250)),
                yOffset="country:N",
                x=alt.X("avg_discount_pct:Q", title="% de descuento sobre precio original"),
                color=alt.Color(
                    "country:N", scale=COUNTRY_COLORS, title="País", legend=alt.Legend(orient="bottom")
                ),
                tooltip=["country", "category_name", alt.Tooltip("avg_discount_pct:Q", format=".1f")],
            )
        )
        st.altair_chart(disc.properties(height=300), use_container_width=True)

# ------------------------------------------------------------------ Tendencias
with tab_trend:
    groups = sorted(mart["category_group"].unique())
    group = st.selectbox("Categoría", groups, index=groups.index("Notebooks") if "Notebooks" in groups else 0)
    trend = mart[mart["category_group"] == group]
    st.subheader(f"Evolución del precio mediano en USD · {group}")
    line = (
        alt.Chart(trend)
        .mark_line(point=True)
        .encode(
            x=alt.X("full_date:T", title=None),
            y=alt.Y("median_price_usd:Q", title="USD", scale=alt.Scale(zero=False)),
            color=alt.Color("country:N", scale=COUNTRY_COLORS, title="País"),
            tooltip=["full_date:T", "country", alt.Tooltip("median_price_usd:Q", format=",.2f"), "items"],
        )
    )
    st.altair_chart(line.properties(height=380), use_container_width=True)

    st.subheader("Publicaciones activas por día")
    vol = mart.groupby(["full_date", "country"], as_index=False)["items"].sum()
    area = (
        alt.Chart(vol)
        .mark_area(opacity=0.8)
        .encode(
            x=alt.X("full_date:T", title=None),
            y=alt.Y("items:Q", title="Publicaciones", stack=True),
            color=alt.Color("country:N", scale=COUNTRY_COLORS, title="País"),
            tooltip=["full_date:T", "country", "items"],
        )
    )
    st.altair_chart(area.properties(height=260), use_container_width=True)
    if len(dates) < 3:
        st.info("Hay pocos días cargados. Corré el backfill para ver tendencias con más historia.")

# ------------------------------------------------------------------ Detalle
with tab_detail:
    left, right = st.columns(2)
    with left:
        st.subheader("Mayores bajas de precio vs. el día anterior")
        changes = load("price_changes", day, sites)
        if changes.empty:
            st.info("Se necesita al menos un día anterior para comparar.")
        else:
            st.dataframe(
                changes.rename(
                    columns={
                        "item_id": "Publicación",
                        "country": "País",
                        "category_name": "Categoría",
                        "prev_price_usd": "Antes (USD)",
                        "price_usd": "Ahora (USD)",
                        "pct_change": "Variación %",
                    }
                ),
                hide_index=True,
                use_container_width=True,
            )
    with right:
        st.subheader("Vendedores con más publicaciones")
        st.dataframe(
            load("top_sellers", day, sites).rename(
                columns={
                    "nickname": "Vendedor",
                    "country": "País",
                    "is_official_store": "Tienda oficial",
                    "publicaciones": "Publicaciones",
                    "precio_promedio_usd": "Precio prom. (USD)",
                }
            ),
            hide_index=True,
            use_container_width=True,
        )
    st.subheader("Mart completo del día")
    st.dataframe(today.drop(columns=["category_group"]), hide_index=True, use_container_width=True)
    st.download_button("⬇️ Descargar CSV", today.to_csv(index=False).encode(), f"mart_{day}.csv", "text/csv")

# ------------------------------------------------------------------ Calidad
with tab_quality:
    dq = load("dq_results", day)
    if dq.empty:
        st.info("No hay resultados de calidad para este día.")
    else:
        ok, total = int(dq["passed"].sum()), len(dq)
        (st.success if ok == total else st.error)(f"{ok} de {total} controles de calidad aprobados")
        dq_view = dq.assign(estado=dq["passed"].map({True: "✅", False: "❌"}))[
            ["estado", "layer", "check_name", "dimension", "severity", "observed", "threshold", "message"]
        ]
        st.dataframe(dq_view, hide_index=True, use_container_width=True)

    left, right = st.columns(2)
    with left:
        st.subheader("Bronze vs. Silver por día")
        st.caption("La diferencia son duplicados eliminados y registros en cuarentena.")
        pv = load("pipeline_volume", start, day)
        if not pv.empty:
            pv_long = pv.melt("run_date", var_name="capa", value_name="filas")
            st.altair_chart(
                alt.Chart(pv_long)
                .mark_line(point=True)
                .encode(
                    x=alt.X("run_date:T", title=None),
                    y=alt.Y("filas:Q", scale=alt.Scale(zero=False)),
                    color=alt.Color("capa:N", title="Capa"),
                    tooltip=["run_date:T", "capa", "filas"],
                )
                .properties(height=280),
                use_container_width=True,
            )
    with right:
        st.subheader("Registros en cuarentena por motivo")
        qr = load("quarantine_by_reason", start, day)
        if not qr.empty:
            st.altair_chart(
                alt.Chart(qr)
                .mark_bar()
                .encode(
                    x=alt.X("snapshot_date:T", title=None),
                    y=alt.Y("registros:Q", stack=True),
                    color=alt.Color("reason:N", title="Motivo"),
                    tooltip=["snapshot_date:T", "reason", "registros"],
                )
                .properties(height=280),
                use_container_width=True,
            )
