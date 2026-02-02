from __future__ import annotations

import sys
from pathlib import Path
import json

import pandas as pd
import streamlit as st
import altair as alt
from sqlalchemy import text

# --- garante raiz do projeto no path ---
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.db import engine  # noqa: E402

st.set_page_config(
    page_title="Análise de Risco — Bancos (IF.data)",
    page_icon="📉",
    layout="wide",
)

# -----------------------------
# Helpers
# -----------------------------
def br_int(n: int) -> str:
    return f"{n:,}".replace(",", ".")

def br_money(x) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "—"
        v = float(x)
        s = f"{v:,.2f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "—"

def br_float(x, nd=2) -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "—"
        return f"{float(x):.{nd}f}".replace(".", ",")
    except Exception:
        return "—"

def safe_json(drivers_raw):
    if drivers_raw is None or (isinstance(drivers_raw, float) and pd.isna(drivers_raw)):
        return None
    try:
        if isinstance(drivers_raw, dict):
            return drivers_raw
        if isinstance(drivers_raw, str):
            return json.loads(drivers_raw)
        return json.loads(str(drivers_raw))
    except Exception:
        return None

@st.cache_data(ttl=60)
def get_available_dates() -> list[str]:
    q = text("SELECT DISTINCT ref_date FROM mart_bank_metrics ORDER BY ref_date DESC")
    with engine.begin() as conn:
        rows = conn.execute(q).fetchall()
    return [str(r[0]) for r in rows]

@st.cache_data(ttl=60)
def load_data(ref_date: str) -> pd.DataFrame:
    q = text("""
      SELECT m.bank_id, m.bank_name,
             m.ativo_total, m.patrimonio_liquido, m.lucro_liquido,
             m.basileia, m.liquidez, m.inadimplencia, m.roa, m.alavancagem,
             r.score, r.rating, r.drivers
      FROM mart_bank_metrics m
      LEFT JOIN mart_bank_risk r
        ON r.ref_date = m.ref_date AND r.bank_id = m.bank_id
      WHERE m.ref_date = :ref_date
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"ref_date": ref_date}).mappings().all()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # tipos numéricos
    for c in ["score", "ativo_total", "patrimonio_liquido", "lucro_liquido", "basileia", "liquidez", "inadimplencia", "roa", "alavancagem"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["rating"] = df["rating"].fillna("SEM_RISCO")
    return df

def rating_color(r: str) -> str:
    # cores no dataframe (funciona bem em Streamlit)
    return {
        "ALTO": "#ff4b4b",
        "MEDIO": "#f7c948",
        "BAIXO": "#2ecc71",
        "SEM_RISCO": "#a0a0a0",
    }.get(r, "#a0a0a0")


# -----------------------------
# UI: Sidebar filters
# -----------------------------
dates = get_available_dates()
if not dates:
    st.warning("Nenhum dado no MART ainda. Rode: ingest_ifdata -> normalize_ifdata -> risk_score.")
    st.stop()

with st.sidebar:
    st.markdown("## ⚙️ Filtros")
    ref_date = st.selectbox("Data-base (ref_date)", dates, index=0)
    rating_filter = st.multiselect(
        "Rating",
        ["ALTO", "MEDIO", "BAIXO", "SEM_RISCO"],
        default=["ALTO", "MEDIO", "BAIXO", "SEM_RISCO"],
    )
    search = st.text_input("Buscar (nome ou id)", value="")
    top_n = st.slider("Top N (ranking)", min_value=10, max_value=200, value=50, step=10)

df = load_data(ref_date)
if df.empty:
    st.warning("Sem dados para essa data. Verifique se o normalize/risk foram executados.")
    st.stop()

# aplica filtros
if rating_filter:
    df = df[df["rating"].isin(rating_filter)]

if search.strip():
    s = search.strip().lower()
    df = df[df["bank_name"].fillna("").str.lower().str.contains(s) | df["bank_id"].fillna("").str.lower().str.contains(s)]

# -----------------------------
# Header
# -----------------------------
st.markdown(
    f"""
    <div style="display:flex; align-items:flex-end; justify-content:space-between;">
      <div>
        <h1 style="margin-bottom:0;">📉 Análise de Risco — Bancos</h1>
        <div style="color:#6b7280; margin-top:4px;">
          Fonte: IF.data (BCB) · Data-base: <b>{ref_date}</b>
        </div>
      </div>
      <div style="text-align:right; color:#6b7280;">
        <div style="font-size:12px;">MART: mart_bank_metrics + mart_bank_risk</div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.divider()

# -----------------------------
# KPIs
# -----------------------------
c1, c2, c3, c4, c5 = st.columns(5)
total = len(df)
alto = int((df["rating"] == "ALTO").sum())
medio = int((df["rating"] == "MEDIO").sum())
baixo = int((df["rating"] == "BAIXO").sum())
sem = int((df["rating"] == "SEM_RISCO").sum())

c1.metric("Bancos", br_int(total))
c2.metric("ALTO", br_int(alto))
c3.metric("MÉDIO", br_int(medio))
c4.metric("BAIXO", br_int(baixo))
c5.metric("Sem risco calc.", br_int(sem))

# Score stats
if df["score"].notna().any():
    st.caption(
        f"Score (0–100): média {br_float(df['score'].mean(),2)} · p90 {br_float(df['score'].quantile(0.9),2)} · max {br_float(df['score'].max(),2)}"
    )

st.divider()

# -----------------------------
# Charts overview
# -----------------------------
left, right = st.columns([1.2, 1])

# rating distribution
rating_counts = (
    df["rating"]
    .value_counts()
    .reindex(["ALTO", "MEDIO", "BAIXO", "SEM_RISCO"])
    .fillna(0)
    .reset_index()
)
rating_counts.columns = ["rating", "count"]

chart_rating = (
    alt.Chart(rating_counts)
    .mark_bar()
    .encode(
        x=alt.X("rating:N", sort=["ALTO", "MEDIO", "BAIXO", "SEM_RISCO"], title="Rating"),
        y=alt.Y("count:Q", title="Qtd"),
        tooltip=["rating", "count"],
    )
    .properties(height=220)
)

# score distribution (hist)
score_df = df[df["score"].notna()][["score"]].copy()
if not score_df.empty:
    chart_score = (
        alt.Chart(score_df)
        .mark_bar()
        .encode(
            x=alt.X("score:Q", bin=alt.Bin(maxbins=30), title="Score (0–100)"),
            y=alt.Y("count():Q", title="Qtd"),
            tooltip=[alt.Tooltip("count():Q", title="Qtd")],
        )
        .properties(height=220)
    )
else:
    chart_score = None

with left:
    st.subheader("Visão geral")
    st.altair_chart(chart_rating, use_container_width=True)
    if chart_score is not None:
        st.altair_chart(chart_score, use_container_width=True)
    else:
        st.info("Sem scores calculados. Rode `poetry run python -m pipelines.risk_score --ref-date ...`.")

with right:
    st.subheader("Relações")
    scatter = df[df["score"].notna()].copy()

    # risco vs basileia (se existir)
    if scatter[["score", "basileia"]].dropna().shape[0] >= 10:
        chart1 = (
            alt.Chart(scatter)
            .mark_circle(size=70)
            .encode(
                x=alt.X("basileia:Q", title="Basileia (%)"),
                y=alt.Y("score:Q", title="Score (0–100)"),
                tooltip=["bank_name", "bank_id", "rating", "score", "basileia"],
            )
            .properties(height=220)
        )
        st.altair_chart(chart1, use_container_width=True)
    else:
        st.caption("Scatter Basileia x Score indisponível (dados insuficientes).")

    # alavancagem vs roa
    if scatter[["alavancagem", "roa"]].dropna().shape[0] >= 10:
        chart2 = (
            alt.Chart(scatter)
            .mark_circle(size=70)
            .encode(
                x=alt.X("alavancagem:Q", title="Alavancagem (Ativo/PL)"),
                y=alt.Y("roa:Q", title="ROA (%)"),
                tooltip=["bank_name", "bank_id", "rating", "score", "alavancagem", "roa"],
            )
            .properties(height=220)
        )
        st.altair_chart(chart2, use_container_width=True)
    else:
        st.caption("Scatter Alavancagem x ROA indisponível (dados insuficientes).")

st.divider()

# -----------------------------
# Ranking table
# -----------------------------
st.subheader(f"📌 Ranking de risco (Top {top_n})")

rank = df.copy()
rank["score"] = pd.to_numeric(rank["score"], errors="coerce")
rank = rank.sort_values(["score", "bank_name"], ascending=[False, True]).head(top_n)

# tabela com colunas principais
table = rank[[
    "bank_id", "bank_name", "rating", "score",
    "basileia", "liquidez", "inadimplencia", "roa", "alavancagem"
]].copy()

# formata
table["score"] = table["score"].round(2)
table["basileia"] = table["basileia"].round(2)
table["liquidez"] = table["liquidez"].round(2)
table["inadimplencia"] = table["inadimplencia"].round(2)
table["roa"] = table["roa"].round(3)
table["alavancagem"] = table["alavancagem"].round(2)

# estilo: barra no score + cor rating
def style_row(row):
    r = row["rating"]
    return [""] * 2 + [f"color: {rating_color(r)}; font-weight:600;"] + [""] * (len(row) - 3)

st.dataframe(
    table.style
        .apply(style_row, axis=1)
        .bar(subset=["score"], vmin=0, vmax=100),
    use_container_width=True,
    hide_index=True
)

st.divider()

# -----------------------------
# Drill-down
# -----------------------------
st.subheader("🔎 Detalhe do banco")

opts = (df["bank_name"].fillna("") + "  —  " + df["bank_id"].fillna("")).tolist()
if not opts:
    st.info("Nenhum banco após filtros.")
    st.stop()

selected = st.selectbox("Selecione", opts, index=0)
bank_id = selected.split("—")[-1].strip()

row = df[df["bank_id"] == bank_id].iloc[0].to_dict()
drivers = safe_json(row.get("drivers"))

a, b = st.columns([1.1, 1])

with a:
    st.markdown(f"### {row.get('bank_name','')}")
    st.caption(f"ID: {row.get('bank_id','')}  ·  Rating: {row.get('rating','—')}  ·  Score: {br_float(row.get('score'),2)}")

    k1, k2, k3 = st.columns(3)
    k1.metric("Ativo Total", br_money(row.get("ativo_total")))
    k2.metric("Patrimônio Líquido", br_money(row.get("patrimonio_liquido")))
    k3.metric("Lucro Líquido", br_money(row.get("lucro_liquido")))

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Basileia (%)", br_float(row.get("basileia"),2))
    m2.metric("Liquidez", br_float(row.get("liquidez"),2))
    m3.metric("Inadimplência (%)", br_float(row.get("inadimplencia"),2))
    m4.metric("ROA (%)", br_float(row.get("roa"),3))
    m5.metric("Alavancagem", br_float(row.get("alavancagem"),2))

with b:
    st.markdown("### Drivers do risco")
    if not drivers:
        st.info("Sem drivers (rode `risk_score` para essa data).")
    else:
        # transforma drivers em df
        drows = []
        for k, v in drivers.items():
            drows.append({"driver": k, "value": v.get("value"), "score": v.get("score")})
        ddf = pd.DataFrame(drows)
        ddf["score"] = pd.to_numeric(ddf["score"], errors="coerce").fillna(0.0)

        # gráfico de contribuição
        chart_drv = (
            alt.Chart(ddf)
            .mark_bar()
            .encode(
                x=alt.X("score:Q", title="Contribuição para o score"),
                y=alt.Y("driver:N", title="", sort="-x"),
                tooltip=["driver", "value", "score"],
            )
            .properties(height=220)
        )
        st.altair_chart(chart_drv, use_container_width=True)
        st.dataframe(ddf, use_container_width=True, hide_index=True)

st.divider()

st.caption(
    "⚠️ Aviso: o score é um *proxy quantitativo* baseado em métricas disponíveis (não é recomendação financeira). "
    "A qualidade depende do mapeamento semântico dos indicadores do IF.data."
)
