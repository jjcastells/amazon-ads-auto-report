import streamlit as st
import pandas as pd
import numpy as np
import re
import unicodedata
from io import BytesIO
from datetime import datetime

# =====================
# Config
# =====================
st.set_page_config(page_title="Amazon Ads MoM Reporter", page_icon="📊", layout="wide")
st.title("📊 Amazon Ads — Monthly Reporting (MoM)")
st.caption("Sube 2 CSV (mes-2 y mes-1) → define tokens (mercados / tipos) → genera un XLSX con reporte inteligente.")

# =====================
# Helpers (estilo BidForest)
# =====================
def strip_accents(s: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", str(s))
        if not unicodedata.combining(ch)
    )

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def _clean(x):
        x = str(x)
        x = x.replace("\ufeff", "")   # BOM
        x = x.replace("\u200b", "")   # zero-width
        x = x.replace("\xa0", " ")    # NBSP -> normal space
        x = re.sub(r"\s+", " ", x)
        return x.strip()

    df.columns = [_clean(c) for c in df.columns]
    return df

def norm_text(x: str) -> str:
    x = str(x)
    x = x.replace("\ufeff", "").replace("\u200b", "").replace("\xa0", " ")
    x = re.sub(r"\s+", " ", x).strip().lower()
    x = strip_accents(x)
    return x

def to_float_euaware(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.replace({"nan": "", "None": "", "<NA>": "", "NaT": ""})

    # limpia moneda/espacios raros
    s = s.str.replace("\xa0", " ", regex=False)
    s = s.str.replace("€", "", regex=False)

    # Detecta si predomina coma decimal
    if (s.str.contains(",").mean() > 0.5):
        s = s.str.replace(r"[^\d,.\-]", "", regex=True)
        s = s.str.replace(".", "", regex=False)   # separador miles
        s = s.str.replace(",", ".", regex=False)  # decimal
    else:
        s = s.str.replace(r"[^\d.\-]", "", regex=True)
        s = s.str.replace(",", "", regex=False)

    return pd.to_numeric(s, errors="coerce").fillna(0.0)

def to_int(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.replace({"nan": "", "None": "", "<NA>": "", "NaT": ""})
    s = s.str.replace(r"[^\d\-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce").fillna(0).astype(int)

def find_col(df: pd.DataFrame, options) -> str | None:
    if isinstance(options, str):
        options = [options]
    cols = list(df.columns)
    cols_norm = {norm_text(c): c for c in cols}
    for opt in options:
        optn = norm_text(opt)
        if optn in cols_norm:
            return cols_norm[optn]
        # soporta columnas con sufijos tipo ".1"
        for cn, original in cols_norm.items():
            if cn.startswith(optn + "."):
                return original
    return None

def sanitize_sheet_name(name: str) -> str:
    cleaned = re.sub(r'[:\\/?*\[\]]', '-', str(name))
    cleaned = cleaned.strip() or "Sheet"
    return cleaned[:31]

def parse_alias_lines(text: str) -> dict[str, list[str]]:
    """
    Formato:
      ES=ES,(ES),Spain
      UK=UK,(UK),GB,(GB)
    Devuelve: {"ES": ["ES","(ES)","SPAIN"], ...} en UPPER
    """
    out = {}
    if not text.strip():
        return out
    for line in text.splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        key, vals = line.split("=", 1)
        key = key.strip().upper()
        aliases = [v.strip() for v in vals.split(",") if v.strip()]
        aliases = [a.upper() for a in aliases]
        if key and aliases:
            out[key] = aliases
    return out

def build_token_map(simple_csv: str, alias_map: dict[str, list[str]]) -> dict[str, list[str]]:
    """
    Si el usuario pone: ES,IT,DE,FR,UK
    y alias_map añade equivalencias, las incorpora.
    """
    base = [x.strip().upper() for x in simple_csv.split(",") if x.strip()]
    token_map = {}
    for t in base:
        token_map[t] = [t]
        if t in alias_map:
            # añade alias extra
            token_map[t] = list(dict.fromkeys([t] + alias_map[t]))
    return token_map

def detect_token(value: str, token_map: dict[str, list[str]]) -> str:
    """
    Busca en el string 'value' si contiene algún alias.
    Devuelve la key canónica o 'UNMAPPED'.
    """
    if value is None:
        return "UNMAPPED"
    haystack = str(value).upper()
    for canon, aliases in token_map.items():
        for a in aliases:
            if a and a in haystack:
                return canon
    return "UNMAPPED"

def detect_many_tokens(value: str, token_map: dict[str, list[str]]) -> list[str]:
    """
    Para casos donde un nombre podría contener múltiples tags,
    aquí devolvemos todas las coincidencias.
    """
    if value is None:
        return []
    haystack = str(value).upper()
    found = []
    for canon, aliases in token_map.items():
        for a in aliases:
            if a and a in haystack:
                found.append(canon)
                break
    return found

def safe_div(a: float, b: float) -> float:
    return float(a) / float(b) if float(b) != 0 else 0.0

# =====================
# Load Amazon CSV
# =====================
@st.cache_data(show_spinner=False)
def load_amz_campaign_csv(file) -> pd.DataFrame:
    df = pd.read_csv(file, dtype=str, encoding="utf-8-sig")
    df = clean_columns(df)
    return df

def standardize_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza columnas clave a nombres canónicos:
      campaign_name, country, ad_type, targeting, impressions, clicks, spend, sales, orders, acos, roas, ctr, cpc
    """
    df = df.copy()

    c_campaign = find_col(df, ["Nombre de la campaña", "Campaign Name"])
    c_country  = find_col(df, ["País", "Country"])
    c_type     = find_col(df, ["Tipo"])
    c_target   = find_col(df, ["Segmentación", "Targeting"])
    c_impr     = find_col(df, ["Impresiones", "Impressions"])
    c_clicks   = find_col(df, ["Clics", "Clicks"])
    c_spend    = find_col(df, ["Coste total (convertido)", "Spend", "Cost"])
    c_sales    = find_col(df, ["Ventas (convertido)", "Sales"])
    c_orders   = find_col(df, ["Compras", "Orders"])
    c_acos     = find_col(df, ["ACOS"])
    c_roas     = find_col(df, ["ROAS"])
    c_ctr      = find_col(df, ["CTR"])
    c_cpc      = find_col(df, ["CPC (convertido)", "CPC"])

    rename = {}
    if c_campaign: rename[c_campaign] = "campaign_name"
    if c_country:  rename[c_country]  = "country"
    if c_type:     rename[c_type]     = "ad_type"       # SP/SB/SD...
    if c_target:   rename[c_target]   = "targeting"     # MANUAL/AUTOMATIC
    if c_impr:     rename[c_impr]     = "impressions"
    if c_clicks:   rename[c_clicks]   = "clicks"
    if c_spend:    rename[c_spend]    = "spend"
    if c_sales:    rename[c_sales]    = "sales"
    if c_orders:   rename[c_orders]   = "orders"
    if c_acos:     rename[c_acos]     = "acos"
    if c_roas:     rename[c_roas]     = "roas"
    if c_ctr:      rename[c_ctr]      = "ctr"
    if c_cpc:      rename[c_cpc]      = "cpc"

    df = df.rename(columns=rename)

    required = ["campaign_name", "spend", "sales", "orders", "clicks", "impressions"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas necesarias: {missing}")

    # métricas numéricas
    for c in ["spend", "sales", "acos", "roas", "ctr", "cpc"]:
        if c in df.columns:
            df[c] = to_float_euaware(df[c])
    for c in ["orders", "clicks", "impressions"]:
        if c in df.columns:
            df[c] = to_int(df[c])

    # Derivadas (blindaje)
    df["ctr_calc"]  = np.where(df["impressions"] > 0, df["clicks"] / df["impressions"], 0.0)
    df["cpc_calc"]  = np.where(df["clicks"] > 0, df["spend"] / df["clicks"], 0.0)
    df["acos_calc"] = np.where(df["sales"] > 0, df["spend"] / df["sales"], 0.0)
    df["roas_calc"] = np.where(df["spend"] > 0, df["sales"] / df["spend"], 0.0)

    return df

def aggregate(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    g = df.groupby(group_cols, dropna=False, as_index=False).agg(
        spend=("spend", "sum"),
        sales=("sales", "sum"),
        orders=("orders", "sum"),
        clicks=("clicks", "sum"),
        impressions=("impressions", "sum"),
    )
    g["acos"] = np.where(g["sales"] > 0, g["spend"] / g["sales"], 0.0)
    g["roas"] = np.where(g["spend"] > 0, g["sales"] / g["spend"], 0.0)
    g["ctr"]  = np.where(g["impressions"] > 0, g["clicks"] / g["impressions"], 0.0)
    g["cpc"]  = np.where(g["clicks"] > 0, g["spend"] / g["clicks"], 0.0)
    return g

def add_mom(prev: pd.DataFrame, curr: pd.DataFrame, keys: list[str], label_prev="prev", label_curr="curr") -> pd.DataFrame:
    p = prev.copy()
    c = curr.copy()

    metric_cols = ["spend","sales","orders","clicks","impressions","acos","roas","ctr","cpc"]

    p = p.rename(columns={m: f"{m}_{label_prev}" for m in metric_cols})
    c = c.rename(columns={m: f"{m}_{label_curr}" for m in metric_cols})

    out = c.merge(p, on=keys, how="outer")
    out = out.fillna(0.0)

    # deltas
    for m in ["spend","sales","orders","clicks","impressions"]:
        out[f"{m}_delta"] = out[f"{m}_{label_curr}"] - out[f"{m}_{label_prev}"]
        base = out[f"{m}_{label_prev}"].replace(0, np.nan)
        out[f"{m}_delta_pct"] = (out[f"{m}_delta"] / base).replace([np.inf, -np.inf], 0).fillna(0.0)

    # ratios (delta absoluto)
    for m in ["acos","roas","ctr","cpc"]:
        out[f"{m}_delta"] = out[f"{m}_{label_curr}"] - out[f"{m}_{label_prev}"]

    return out

# =====================
# UI: uploads
# =====================
st.subheader("1) Sube los CSV")
c1, c2 = st.columns(2)
with c1:
    file_prev = st.file_uploader("📤 Mes anterior al anterior (ej: diciembre 2025)", type=["csv"], key="prev")
with c2:
    file_curr = st.file_uploader("📤 Mes anterior (ej: enero 2026)", type=["csv"], key="curr")

if not file_prev or not file_curr:
    st.info("Sube ambos CSV para continuar.")
    st.stop()

df_prev_raw = load_amz_campaign_csv(file_prev)
df_curr_raw = load_amz_campaign_csv(file_curr)

# =====================
# UI: tokens (universal)
# =====================
st.subheader("2) Define cómo detectar Mercados y Tipos desde el nombre de campaña")

cc1, cc2 = st.columns(2)
with cc1:
    markets_simple = st.text_input("Mercados (canónicos) separados por coma", value="ES,IT,DE,FR,UK")
    markets_alias = st.text_area(
        "Aliases de mercados (opcional). Formato: CANON=alias1,alias2,... (1 por línea)",
        value="UK=GB,(GB),(UK)\nDE=ALE",
        height=120
    )
with cc2:
    tags_simple = st.text_input("Tipos/Tags de campaña separados por coma (internos)", value="NB,BR,AUTO,PAT,KW")
    tags_alias = st.text_area(
        "Aliases de tags (opcional). Formato: CANON=alias1,alias2,... (1 por línea)",
        value="AUTO=AUTOMATIC,AUT\nBR=BRANDED\nNB=NON-BRANDED,NONBRAND",
        height=120
    )

markets_map = build_token_map(markets_simple, parse_alias_lines(markets_alias))
tags_map    = build_token_map(tags_simple, parse_alias_lines(tags_alias))

# =====================
# Prepare data
# =====================
try:
    df_prev = standardize_metrics(df_prev_raw)
    df_curr = standardize_metrics(df_curr_raw)
except Exception as e:
    st.error(f"Error estandarizando columnas: {e}")
    st.stop()

# Detect market/tag from campaign name
for df in (df_prev, df_curr):
    df["market"] = df["campaign_name"].apply(lambda x: detect_token(x, markets_map))
    # puede haber múltiples tags, pero ponemos principal (primera coincidencia) y también lista
    df["tags_found"] = df["campaign_name"].apply(lambda x: detect_many_tokens(x, tags_map))
    df["camp_tag"] = df["tags_found"].apply(lambda lst: lst[0] if isinstance(lst, list) and len(lst) else "UNMAPPED")

# =====================
# Preview
# =====================
st.subheader("3) Vista previa + validación de parsing")
p1, p2 = st.columns(2)
with p1:
    st.caption("Mes-2 (prev)")
    st.dataframe(df_prev[["campaign_name","market","camp_tag","spend","sales","orders"]].head(20), use_container_width=True)
with p2:
    st.caption("Mes-1 (curr)")
    st.dataframe(df_curr[["campaign_name","market","camp_tag","spend","sales","orders"]].head(20), use_container_width=True)

bad_prev = (df_prev["market"].eq("UNMAPPED").mean()) * 100
bad_curr = (df_curr["market"].eq("UNMAPPED").mean()) * 100
st.info(f"UNMAPPED market: prev {bad_prev:.1f}% · curr {bad_curr:.1f}%  (si es alto, añade aliases o ajusta tokens)")

# =====================
# Button: generate
# =====================
st.divider()
run = st.button("🚀 Generar reporte XLSX", use_container_width=True)
if not run:
    st.stop()

# =====================
# Aggregations
# =====================
# Global summary
glob_prev = aggregate(df_prev, group_cols=["__all__".replace("__all__","market")])  # dummy to keep function simple
glob_curr = aggregate(df_curr, group_cols=["__all__".replace("__all__","market")])
# hack: above groups by 'market' - we want global totals too:
global_prev = pd.DataFrame([{
    "spend": df_prev["spend"].sum(),
    "sales": df_prev["sales"].sum(),
    "orders": df_prev["orders"].sum(),
    "clicks": df_prev["clicks"].sum(),
    "impressions": df_prev["impressions"].sum(),
}])
global_prev["acos"] = safe_div(global_prev["spend"][0], global_prev["sales"][0])
global_prev["roas"] = safe_div(global_prev["sales"][0], global_prev["spend"][0])
global_prev["ctr"]  = safe_div(global_prev["clicks"][0], global_prev["impressions"][0])
global_prev["cpc"]  = safe_div(global_prev["spend"][0], global_prev["clicks"][0])

global_curr = pd.DataFrame([{
    "spend": df_curr["spend"].sum(),
    "sales": df_curr["sales"].sum(),
    "orders": df_curr["orders"].sum(),
    "clicks": df_curr["clicks"].sum(),
    "impressions": df_curr["impressions"].sum(),
}])
global_curr["acos"] = safe_div(global_curr["spend"][0], global_curr["sales"][0])
global_curr["roas"] = safe_div(global_curr["sales"][0], global_curr["spend"][0])
global_curr["ctr"]  = safe_div(global_curr["clicks"][0], global_curr["impressions"][0])
global_curr["cpc"]  = safe_div(global_curr["spend"][0], global_curr["clicks"][0])

# Groupings
by_market_prev = aggregate(df_prev, ["market"])
by_market_curr = aggregate(df_curr, ["market"])
by_tag_prev    = aggregate(df_prev, ["camp_tag"])
by_tag_curr    = aggregate(df_curr, ["camp_tag"])
by_mkt_tag_prev = aggregate(df_prev, ["market","camp_tag"])
by_mkt_tag_curr = aggregate(df_curr, ["market","camp_tag"])

# Campaign-level MoM
camp_prev = aggregate(df_prev, ["campaign_name","market","camp_tag"])
camp_curr = aggregate(df_curr, ["campaign_name","market","camp_tag"])
camp_mom  = add_mom(camp_prev, camp_curr, keys=["campaign_name","market","camp_tag"], label_prev="prev", label_curr="curr")

# Sort useful views
camp_mom_spend = camp_mom.sort_values("spend_delta", ascending=False)
camp_mom_sales = camp_mom.sort_values("sales_delta", ascending=False)

# Insights simple (ejemplos)
insights = []
total_spend_delta = float(global_curr["spend"][0] - global_prev["spend"][0])
total_sales_delta = float(global_curr["sales"][0] - global_prev["sales"][0])
acos_prev = float(global_prev["acos"][0]); acos_curr = float(global_curr["acos"][0])

insights.append(f"Spend total Δ: {total_spend_delta:,.2f} €")
insights.append(f"Sales total Δ: {total_sales_delta:,.2f} €")
insights.append(f"ACOS Δ: {(acos_curr - acos_prev) * 100:.2f} pp")

# Detect “spend up, sales down” campaigns (top 10)
w = camp_mom[(camp_mom["spend_delta"] > 0) & (camp_mom["sales_delta"] < 0)].copy()
w = w.sort_values("spend_delta", ascending=False).head(10)
if len(w):
    insights.append("Top campañas con Spend ↑ y Sales ↓ (revisar):")
    for _, r in w.iterrows():
        insights.append(f" - {r['campaign_name']} | SpendΔ {r['spend_delta']:,.2f} | SalesΔ {r['sales_delta']:,.2f}")

insights_df = pd.DataFrame({"insight": insights})

# =====================
# UI metrics quick
# =====================
st.subheader("✅ Preview del reporte (rápido)")
m1, m2, m3, m4 = st.columns(4)

spend_prev = float(global_prev["spend"][0]); spend_curr = float(global_curr["spend"][0])
sales_prev = float(global_prev["sales"][0]); sales_curr = float(global_curr["sales"][0])
orders_prev = int(global_prev["orders"][0]); orders_curr = int(global_curr["orders"][0])

acos_prev = float(global_prev["acos"][0]); acos_curr = float(global_curr["acos"][0])
roas_prev = float(global_prev["roas"][0]); roas_curr = float(global_curr["roas"][0])

total_spend_delta = spend_curr - spend_prev
total_sales_delta = sales_curr - sales_prev
orders_delta = orders_curr - orders_prev
acos_delta_pp = (acos_curr - acos_prev) * 100  # puntos porcentuales

m1.metric("Spend (curr)", f"{spend_curr:,.2f} €", f"{total_spend_delta:,.2f} €")
m2.metric("Sales (curr)", f"{sales_curr:,.2f} €", f"{total_sales_delta:,.2f} €")
m3.metric("ACOS (curr)", f"{acos_curr*100:.2f} %", f"{acos_delta_pp:.2f} pp")
m4.metric("Orders (curr)", f"{orders_curr:,}", f"{orders_delta:,}")

tabs = st.tabs(["By Market", "By Tag", "Market x Tag", "Campaign MoM", "Insights"])
with tabs[0]:
    st.dataframe(add_mom(by_market_prev, by_market_curr, ["market"]), use_container_width=True)
with tabs[1]:
    st.dataframe(add_mom(by_tag_prev, by_tag_curr, ["camp_tag"]), use_container_width=True)
with tabs[2]:
    st.dataframe(add_mom(by_mkt_tag_prev, by_mkt_tag_curr, ["market","camp_tag"]), use_container_width=True)
with tabs[3]:
    st.caption("Ordenado por SpendΔ (desc).")
    st.dataframe(camp_mom_spend.head(50), use_container_width=True)
with tabs[4]:
    st.dataframe(insights_df, use_container_width=True)

# =====================
# Export XLSX
# =====================
st.divider()
st.subheader("💾 Descargar XLSX")

output = BytesIO()
ts = datetime.now().strftime("%Y-%m-%d")
with pd.ExcelWriter(output, engine="openpyxl") as writer:
    # Global
    gp = global_prev.copy(); gp.insert(0, "period", "prev")
    gc = global_curr.copy(); gc.insert(0, "period", "curr")
    global_sheet = pd.concat([gp, gc], ignore_index=True)
    global_sheet.to_excel(writer, index=False, sheet_name=sanitize_sheet_name("01_Summary_Global"))

    add_mom(by_market_prev, by_market_curr, ["market"]).to_excel(writer, index=False, sheet_name=sanitize_sheet_name("02_By_Market"))
    add_mom(by_tag_prev, by_tag_curr, ["camp_tag"]).to_excel(writer, index=False, sheet_name=sanitize_sheet_name("03_By_Tag"))
    add_mom(by_mkt_tag_prev, by_mkt_tag_curr, ["market","camp_tag"]).to_excel(writer, index=False, sheet_name=sanitize_sheet_name("04_Market_x_Tag"))

    camp_mom.to_excel(writer, index=False, sheet_name=sanitize_sheet_name("05_Campaign_MoM"))
    camp_mom_spend.head(100).to_excel(writer, index=False, sheet_name=sanitize_sheet_name("06_Top_Spend_Movers"))
    camp_mom_sales.head(100).to_excel(writer, index=False, sheet_name=sanitize_sheet_name("07_Top_Sales_Movers"))
    insights_df.to_excel(writer, index=False, sheet_name=sanitize_sheet_name("99_Insights"))

st.download_button(
    label="⬇️ Descargar AmazonAds_MoM_Report.xlsx",
    data=output.getvalue(),
    file_name=f"AmazonAds_MoM_Report_{ts}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True
)
