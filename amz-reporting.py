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
st.caption("Sube 2 CSV (mes-2 y mes-1) → define tokens (mercados / tipos) → genera un XLSX con reporte inteligente + resumen cliente + email.")

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

    s = s.str.replace("\xa0", " ", regex=False)
    s = s.str.replace("€", "", regex=False)

    if (s.str.contains(",").mean() > 0.5):
        s = s.str.replace(r"[^\d,.\-]", "", regex=True)
        s = s.str.replace(".", "", regex=False)
        s = s.str.replace(",", ".", regex=False)
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
        for cn, original in cols_norm.items():
            if cn.startswith(optn + "."):
                return original
    return None

def sanitize_sheet_name(name: str) -> str:
    cleaned = re.sub(r'[:\\/?*\[\]]', '-', str(name))
    cleaned = cleaned.strip() or "Sheet"
    return cleaned[:31]

def safe_sheet_prefix(client: str) -> str:
    c = re.sub(r"[^A-Za-z0-9\s\-_]", "", str(client)).strip()
    c = re.sub(r"\s+", " ", c)
    if not c:
        c = "CLIENT"
    return c[:12]

def parse_alias_lines(text: str) -> dict[str, list[str]]:
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
    base = [x.strip().upper() for x in simple_csv.split(",") if x.strip()]
    token_map = {}
    for t in base:
        token_map[t] = [t]
        if t in alias_map:
            token_map[t] = list(dict.fromkeys([t] + alias_map[t]))
    return token_map

def detect_token(value: str, token_map: dict[str, list[str]]) -> str:
    if value is None:
        return "UNMAPPED"
    haystack = str(value).upper()
    for canon, aliases in token_map.items():
        for a in aliases:
            if a and a in haystack:
                return canon
    return "UNMAPPED"

def detect_many_tokens(value: str, token_map: dict[str, list[str]]) -> list[str]:
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

def fmt_eur_es(x: float) -> str:
    s = f"{float(x):,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{s} €"

def fmt_int_es(x: float | int) -> str:
    s = f"{int(x):,}"
    return s.replace(",", ".")

def fmt_pp_es(pp: float) -> str:
    s = f"{pp:+.2f}".replace(".", ",")
    return f"{s} pp"

def fmt_pct_es(ratio: float) -> str:
    s = f"{ratio*100:.2f}".replace(".", ",")
    return f"{s}%"

def fmt_num_es(x: float) -> str:
    return f"{float(x):.2f}".replace(".", ",")

def fmt_delta_eur_es(delta: float, label_prev: str) -> str:
    sign = "+" if delta >= 0 else ""
    return f"(Δ {sign}{fmt_eur_es(delta).replace(' €','')} € vs {label_prev})"

def fmt_delta_int_es(delta: int, label_prev: str) -> str:
    sign = "+" if delta >= 0 else ""
    return f"(Δ {sign}{fmt_int_es(delta)} vs {label_prev})"

# =====================
# Client-friendly layer
# =====================
SPANISH_MONTHS = [
    "Enero","Febrero","Marzo","Abril","Mayo","Junio",
    "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
]

def build_client_kpis(global_prev: pd.DataFrame, global_curr: pd.DataFrame) -> pd.DataFrame:
    spend_prev = float(global_prev["spend"][0]); spend_curr = float(global_curr["spend"][0])
    sales_prev = float(global_prev["sales"][0]); sales_curr = float(global_curr["sales"][0])
    orders_prev = int(global_prev["orders"][0]); orders_curr = int(global_curr["orders"][0])
    clicks_prev = int(global_prev["clicks"][0]); clicks_curr = int(global_curr["clicks"][0])
    impr_prev = int(global_prev["impressions"][0]); impr_curr = int(global_curr["impressions"][0])

    acos_prev = float(global_prev["acos"][0]); acos_curr = float(global_curr["acos"][0])
    roas_prev = float(global_prev["roas"][0]); roas_curr = float(global_curr["roas"][0])
    ctr_prev = float(global_prev["ctr"][0]); ctr_curr = float(global_curr["ctr"][0])
    cpc_prev = float(global_prev["cpc"][0]); cpc_curr = float(global_curr["cpc"][0])

    # NUEVO: CR + CAC (ads)
    cr_prev = safe_div(orders_prev, clicks_prev)
    cr_curr = safe_div(orders_curr, clicks_curr)
    cac_prev = safe_div(spend_prev, orders_prev)  # coste por pedido (ads)
    cac_curr = safe_div(spend_curr, orders_curr)

    def pct_delta(curr, prev):
        return safe_div(curr - prev, prev) if float(prev) != 0 else 0.0

    rows = [
        ["Spend", spend_curr, spend_prev, spend_curr - spend_prev, pct_delta(spend_curr, spend_prev), "€"],
        ["Sales", sales_curr, sales_prev, sales_curr - sales_prev, pct_delta(sales_curr, sales_prev), "€"],
        ["Orders", orders_curr, orders_prev, orders_curr - orders_prev, pct_delta(orders_curr, orders_prev), "#"],
        ["ACOS", acos_curr, acos_prev, acos_curr - acos_prev, 0.0, "ratio"],
        ["ROAS", roas_curr, roas_prev, roas_curr - roas_prev, 0.0, "ratio"],
        ["CR", cr_curr, cr_prev, cr_curr - cr_prev, 0.0, "ratio"],
        ["CAC", cac_curr, cac_prev, cac_curr - cac_prev, 0.0, "€"],
        ["CTR", ctr_curr, ctr_prev, ctr_curr - ctr_prev, 0.0, "ratio"],
        ["CPC", cpc_curr, cpc_prev, cpc_curr - cpc_prev, 0.0, "€"],
        ["Clicks", clicks_curr, clicks_prev, clicks_curr - clicks_prev, pct_delta(clicks_curr, clicks_prev), "#"],
        ["Impressions", impr_curr, impr_prev, impr_curr - impr_prev, pct_delta(impr_curr, impr_prev), "#"],
    ]
    return pd.DataFrame(rows, columns=["KPI","Current","Previous","Delta","DeltaPct","Unit"])

def pick_top_watchlist(camp_mom: pd.DataFrame, top_n=3) -> pd.DataFrame:
    w = camp_mom[(camp_mom["spend_delta"] > 0) & (camp_mom["sales_delta"] < 0)].copy()
    w = w.sort_values("spend_delta", ascending=False).head(top_n)
    cols = ["campaign_name","market","camp_tag","spend_delta","sales_delta","acos_delta","cr_delta","cac_delta"]
    cols = [c for c in cols if c in w.columns]
    return w[cols]

def pick_top_winners(camp_mom: pd.DataFrame, top_n=3) -> pd.DataFrame:
    w = camp_mom[(camp_mom["sales_delta"] > 0) & (camp_mom["acos_delta"] <= 0)].copy()
    w = w.sort_values("sales_delta", ascending=False).head(top_n)
    cols = ["campaign_name","market","camp_tag","sales_delta","spend_delta","acos_delta","cr_delta","cac_delta"]
    cols = [c for c in cols if c in w.columns]
    return w[cols]

# NUEVO: prioridades de optimización por ACOS (campaign-level)
def pick_top_acos_priorities(
    camp_mom: pd.DataFrame,
    top_n: int = 3,
    min_spend_curr: float = 50.0
) -> pd.DataFrame:
    """
    Top campañas (campaign_name x market x camp_tag) con mayor subida de ACOS (Δ pp).
    Filtra por spend_curr mínimo para evitar ruido.
    """
    if camp_mom is None or camp_mom.empty:
        return pd.DataFrame(columns=["campaign_name","market","camp_tag","acos_delta_pp"])

    required = ["campaign_name","market","camp_tag","acos_curr","acos_prev","spend_curr"]
    for c in required:
        if c not in camp_mom.columns:
            return pd.DataFrame(columns=["campaign_name","market","camp_tag","acos_delta_pp"])

    df = camp_mom.copy()
    df["acos_delta_pp"] = (df["acos_curr"] - df["acos_prev"]) * 100.0

    # ruido fuera: solo campañas con inversión mínima y ACOS empeora (sube)
    df = df[df["spend_curr"] >= float(min_spend_curr)].copy()
    df = df[df["acos_delta_pp"] > 0].copy()

    if df.empty:
        return pd.DataFrame(columns=["campaign_name","market","camp_tag","acos_delta_pp"])

    df = df.sort_values(["acos_delta_pp"], ascending=[False]).head(top_n)

    return df[["campaign_name","market","camp_tag","acos_delta_pp"]]

def _neutral_direction(value: float, eps: float = 1e-12) -> str:
    if value > eps:
        return "sube"
    if value < -eps:
        return "baja"
    return "se mantiene"

def build_client_insights(global_prev: pd.DataFrame,
                         global_curr: pd.DataFrame,
                         by_market_prev: pd.DataFrame,
                         by_market_curr: pd.DataFrame,
                         camp_mom: pd.DataFrame,
                         period_prev_label: str,
                         period_curr_label: str) -> list[dict]:

    spend_prev = float(global_prev["spend"][0]); spend_curr = float(global_curr["spend"][0])
    sales_prev = float(global_prev["sales"][0]); sales_curr = float(global_curr["sales"][0])
    orders_prev = int(global_prev["orders"][0]); orders_curr = int(global_curr["orders"][0])
    clicks_prev = int(global_prev["clicks"][0]); clicks_curr = int(global_curr["clicks"][0])

    acos_prev = float(global_prev["acos"][0]); acos_curr = float(global_curr["acos"][0])

    spend_delta = spend_curr - spend_prev
    sales_delta = sales_curr - sales_prev
    orders_delta = orders_curr - orders_prev
    acos_delta_pp = (acos_curr - acos_prev) * 100

    # CR + CAC (ads)
    cr_prev = safe_div(orders_prev, clicks_prev)
    cr_curr = safe_div(orders_curr, clicks_curr)
    cr_delta_pp = (cr_curr - cr_prev) * 100

    cac_prev = safe_div(spend_prev, orders_prev)
    cac_curr = safe_div(spend_curr, orders_curr)
    cac_delta = cac_curr - cac_prev

    # driver por spend (market)
    mom_mkt = add_mom(by_market_prev, by_market_curr, ["market"])
    mom_mkt = mom_mkt.sort_values("spend_delta", ascending=False)
    top_mkt_spend = mom_mkt.head(1).iloc[0].to_dict() if len(mom_mkt) else None

    # listas
    watch = pick_top_watchlist(camp_mom, top_n=3)
    wins = pick_top_winners(camp_mom, top_n=3)

    insights = []

    # 1) Foto del mes (neutral)
    insights.append({
        "Title": "Foto del mes (resumen)",
        "What": (
            f"Comparando {period_curr_label} vs {period_prev_label}: "
            f"Spend Δ {fmt_eur_es(spend_delta)}, Sales Δ {fmt_eur_es(sales_delta)}, Orders Δ {fmt_int_es(orders_delta)}. "
            f"ACOS {_neutral_direction(acos_delta_pp)} ({acos_delta_pp:+.2f} pp). "
            f"CR {_neutral_direction(cr_delta_pp)} ({cr_delta_pp:+.2f} pp). "
            f"CAC (ads) {_neutral_direction(cac_delta)} ({fmt_eur_es(cac_delta)})."
        )
    })

    # 2) Prioridades ACOS (Top 3) - solo mostramos Δ ACOS
    acos_prio = pick_top_acos_priorities(camp_mom, top_n=3, min_spend_curr=50.0)

    if not acos_prio.empty:
        items = []
        for _, r in acos_prio.iterrows():
            name = str(r["campaign_name"])
            mkt = str(r.get("market",""))
            tag = str(r.get("camp_tag",""))
            pp = float(r.get("acos_delta_pp", 0.0))
            items.append(f"{name} ({mkt} | {tag}): Δ ACOS {fmt_pp_es(pp)}")

        insights.append({
            "Title": "Prioridades de optimización (ACOS)",
            "What": "Top 3 campañas foco (por subida de ACOS vs mes anterior): " + "; ".join(items) + "."
        })
    else:
        insights.append({
            "Title": "Prioridades de optimización (ACOS)",
            "What": "No se detectan subidas relevantes de ACOS con inversión suficiente (por encima del umbral)."
        })

    # 3) CR
    if abs(cr_delta_pp) >= 0.10:
        insights.append({
            "Title": "Calidad de tráfico (CR)",
            "What": (
                f"El CR {_neutral_direction(cr_delta_pp)} ({cr_delta_pp:+.2f} pp). "
                "Lo desglosamos por mercado y tipo de campaña para aislar el origen del cambio."
            )
        })
    else:
        insights.append({
            "Title": "Calidad de tráfico (CR)",
            "What": (
                "El CR se mantiene estable (variación pequeña). "
                "Los cambios se explican más por mix de inversión/pujas que por conversión."
            )
        })

    # 4) CAC (ads)
    if abs(cac_delta) >= 0.05:
        insights.append({
            "Title": "Coste por pedido (CAC ads)",
            "What": (
                f"El CAC (ads) {_neutral_direction(cac_delta)} ({fmt_eur_es(cac_delta)}). "
                "Lo usamos como referencia rápida del coste por pedido atribuido a Ads."
            )
        })
    else:
        insights.append({
            "Title": "Coste por pedido (CAC ads)",
            "What": "CAC (ads) estable (variación pequeña). Mantendremos consistencia y ajustes graduales."
        })

    # 5) Driver principal por inversión (market)
    if top_mkt_spend:
        mkt = top_mkt_spend.get("market", "N/A")
        sd = float(top_mkt_spend.get("spend_delta", 0.0))
        sld = float(top_mkt_spend.get("sales_delta", 0.0))
        insights.append({
            "Title": "Driver principal por inversión",
            "What": f"{mkt} concentra el mayor cambio (Spend {fmt_eur_es(sd)} con Sales {fmt_eur_es(sld)})."
        })

    # 6) Campañas (neutro)
    if len(watch):
        items = [f"{r['campaign_name']} ({r.get('market','')})" for _, r in watch.iterrows()]
        insights.append({
            "Title": "Campañas a monitorizar",
            "What": "Sube la inversión y caen las ventas en: " + "; ".join(items) + "."
        })

    if len(wins):
        items = [f"{r['campaign_name']} ({r.get('market','')})" for _, r in wins.iterrows()]
        insights.append({
            "Title": "Campañas con tracción",
            "What": "Ventas al alza con ACOS estable/bajando en: " + "; ".join(items) + "."
        })

    return insights[:7]

def build_client_actions(global_prev: pd.DataFrame,
                         global_curr: pd.DataFrame,
                         camp_mom: pd.DataFrame) -> list[str]:
    spend_prev = float(global_prev["spend"][0]); spend_curr = float(global_curr["spend"][0])
    sales_prev = float(global_prev["sales"][0]); sales_curr = float(global_curr["sales"][0])
    orders_prev = int(global_prev["orders"][0]); orders_curr = int(global_curr["orders"][0])
    clicks_prev = int(global_prev["clicks"][0]); clicks_curr = int(global_curr["clicks"][0])

    acos_prev = float(global_prev["acos"][0]); acos_curr = float(global_curr["acos"][0])
    cr_prev = safe_div(orders_prev, clicks_prev)
    cr_curr = safe_div(orders_curr, clicks_curr)
    cac_prev = safe_div(spend_prev, orders_prev)
    cac_curr = safe_div(spend_curr, orders_curr)

    acos_delta_pp = (acos_curr - acos_prev) * 100
    cr_delta_pp = (cr_curr - cr_prev) * 100
    cac_delta = cac_curr - cac_prev
    sales_delta = sales_curr - sales_prev

    actions = []

    if sales_delta >= 0 and acos_delta_pp <= 0:
        actions.append("Mantener y escalar gradualmente donde se confirma tracción (ventas al alza con ACOS estable/bajando).")
    elif sales_delta >= 0 and acos_delta_pp > 0:
        actions.append("Consolidar crecimiento controlando el mix (priorizar eficiencia en campañas con mayor crecimiento de inversión).")
    elif sales_delta < 0 and acos_delta_pp <= 0:
        actions.append("Recuperar volumen sin alterar demasiado el mix (revisar cobertura y términos con caída de pedidos/ventas).")
    else:
        actions.append("Priorizar estabilidad: aislar cambios por mercado/campaña y ajustar de forma incremental en las áreas con mayor impacto.")

    actions.append("Higiene de cuenta: revisar campañas/targets con inversión creciente sin ventas (negatives, limpieza de targets, pausas tácticas).")

    if abs(cr_delta_pp) >= 0.10:
        actions.append("Revisar CR por mercado/tipo (términos de búsqueda, segmentación y páginas de producto) para aislar el origen del cambio.")
    else:
        actions.append("Mantener seguimiento de CR (estable) y centrar ajustes en inversión y estructura de campañas.")

    if abs(cac_delta) >= 0.05:
        actions.append("Monitorizar CAC (ads) y ajustar presupuesto/pujas en función de campañas con mejor coste por pedido.")
    else:
        actions.append("CAC (ads) estable: continuar con escalado/ajustes graduales manteniendo consistencia.")

    actions.append("Ajuste fino: revisar distribución por tipo (NB/BR/AUTO) para equilibrar volumen y consistencia en la relación inversión/ventas.")

    return actions[:5]

def format_saludo_poc(poc: str) -> str:
    poc = str(poc).strip()
    if not poc:
        return "Hola,"
    poc = re.sub(r"\s*,\s*", ", ", poc)
    return f"Hola {poc},"

def generate_client_email_es_plain(
    client: str,
    poc: str,
    period_prev_label: str,
    period_curr_label: str,
    client_kpis: pd.DataFrame,
    insights_list: list[dict],
    actions_list: list[str],
    sender_name: str
) -> tuple[str, str]:

    subject = f"{client} | Amazon Ads — Reporte {period_curr_label} (vs {period_prev_label})"

    def get_row(kpi):
        return client_kpis[client_kpis["KPI"] == kpi].iloc[0]

    spend = get_row("Spend")
    sales = get_row("Sales")
    orders = get_row("Orders")
    acos = get_row("ACOS")
    roas = get_row("ROAS")
    cr = get_row("CR")
    cac = get_row("CAC")

    kpi_lines = [
        f"- Spend: {fmt_eur_es(spend['Current'])} {fmt_delta_eur_es(float(spend['Delta']), period_prev_label)}",
        f"- Sales: {fmt_eur_es(sales['Current'])} {fmt_delta_eur_es(float(sales['Delta']), period_prev_label)}",
        f"- Orders: {fmt_int_es(orders['Current'])} {fmt_delta_int_es(int(orders['Delta']), period_prev_label)}",
        f"- ACOS: {fmt_pct_es(float(acos['Current']))} (Δ {fmt_pp_es(float(acos['Delta']) * 100)} vs {period_prev_label})",
        f"- ROAS: {fmt_num_es(float(roas['Current']))} (Δ {fmt_num_es(float(roas['Delta']))} vs {period_prev_label})",
        f"- CR: {fmt_pct_es(float(cr['Current']))} (Δ {fmt_pp_es(float(cr['Delta']) * 100)} vs {period_prev_label})",
        f"- CAC (ads): {fmt_eur_es(float(cac['Current']))} {fmt_delta_eur_es(float(cac['Delta']), period_prev_label)}",
    ]

    insight_lines = []
    for it in insights_list[:4]:
        title = str(it.get("Title", "")).strip()
        what = str(it.get("What", "")).strip()
        if title and what:
            insight_lines.append(f"- {title}: {what}")
        elif what:
            insight_lines.append(f"- {what}")

    if not insight_lines:
        insight_lines = ["- (Sin insights destacados este mes)"]

    action_lines = [f"- {a}" for a in actions_list[:4]] if actions_list else ["- (Sin acciones definidas)"]

    sep = "--------------------------------"
    saludo = format_saludo_poc(poc)

    body = "\n".join([
        f"{saludo}",
        "",
        f"Te comparto el reporte de {client} para {period_curr_label} (comparado con {period_prev_label}).",
        "Te dejo lo más importante, en corto:",
        "",
        sep,
        "KPIs CLAVE",
        sep,
        *kpi_lines,
        "",
        sep,
        "INSIGHTS PRINCIPALES",
        sep,
        *insight_lines,
        "",
        sep,
        "PRÓXIMAS ACCIONES",
        sep,
        *action_lines,
        "",
        "Adjunto Excel con el detalle por mercados y tipos de campaña.",
        "",
        "Si te va bien, lo vemos 10 minutos y te cuento exactamente qué estamos ajustando.",
        "",
        "Un abrazo,",
        f"{sender_name}"
    ])

    return subject, body

# =====================
# Load Amazon CSV
# =====================
@st.cache_data(show_spinner=False)
def load_amz_campaign_csv(file) -> pd.DataFrame:
    df = pd.read_csv(file, dtype=str, encoding="utf-8-sig")
    df = clean_columns(df)
    return df

def standardize_metrics(df: pd.DataFrame) -> pd.DataFrame:
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
    if c_type:     rename[c_type]     = "ad_type"
    if c_target:   rename[c_target]   = "targeting"
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

    for c in ["spend", "sales", "acos", "roas", "ctr", "cpc"]:
        if c in df.columns:
            df[c] = to_float_euaware(df[c])
    for c in ["orders", "clicks", "impressions"]:
        if c in df.columns:
            df[c] = to_int(df[c])

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

    g["cr"] = np.where(g["clicks"] > 0, g["orders"] / g["clicks"], 0.0)
    g["cac"] = np.where(g["orders"] > 0, g["spend"] / g["orders"], 0.0)

    return g

def add_mom(prev: pd.DataFrame, curr: pd.DataFrame, keys: list[str], label_prev="prev", label_curr="curr") -> pd.DataFrame:
    p = prev.copy()
    c = curr.copy()

    metric_cols = ["spend","sales","orders","clicks","impressions","acos","roas","ctr","cpc","cr","cac"]

    p = p.rename(columns={m: f"{m}_{label_prev}" for m in metric_cols})
    c = c.rename(columns={m: f"{m}_{label_curr}" for m in metric_cols})

    out = c.merge(p, on=keys, how="outer")
    out = out.fillna(0.0)

    for m in ["spend","sales","orders","clicks","impressions"]:
        out[f"{m}_delta"] = out[f"{m}_{label_curr}"] - out[f"{m}_{label_prev}"]
        base = out[f"{m}_{label_prev}"].replace(0, np.nan)
        out[f"{m}_delta_pct"] = (out[f"{m}_delta"] / base).replace([np.inf, -np.inf], 0).fillna(0.0)

    for m in ["acos","roas","ctr","cpc","cr","cac"]:
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
# UI: period labels (para XLSX + email)
# =====================
st.subheader("1.5) Periodos del reporte (para naming + email)")
pc1, pc2, pc3 = st.columns([1, 1, 1])
with pc1:
    prev_month = st.selectbox("Mes prev (mes-2)", SPANISH_MONTHS, index=11)
with pc2:
    prev_year = st.number_input("Año prev", min_value=2000, max_value=2100, value=2025, step=1)
with pc3:
    st.text_input("Etiqueta prev (auto)", value=f"{prev_month} {int(prev_year)}", disabled=True)

cc1p, cc2p, cc3p = st.columns([1, 1, 1])
with cc1p:
    curr_month = st.selectbox("Mes curr (mes-1)", SPANISH_MONTHS, index=0)
with cc2p:
    curr_year = st.number_input("Año curr", min_value=2000, max_value=2100, value=2026, step=1)
with cc3p:
    st.text_input("Etiqueta curr (auto)", value=f"{curr_month} {int(curr_year)}", disabled=True)

period_prev_label = f"{prev_month} {int(prev_year)}"
period_curr_label = f"{curr_month} {int(curr_year)}"

# =====================
# UI: CLIENT + POC
# =====================
st.subheader("1.6) Datos del cliente")
d1, d2 = st.columns(2)
with d1:
    CLIENT = st.text_input("CLIENT (Nombre de la empresa)", value="CLIENT")
with d2:
    POC = st.text_input("POC (Persona de contacto)", value="")

client_prefix = safe_sheet_prefix(CLIENT)

# =====================
# UI: tokens (universal)
# =====================
st.subheader("2) Define cómo detectar Mercados y Tipos desde el nombre de campaña")

cc1, cc2 = st.columns(2)
with cc1:
    markets_simple = st.text_input("Mercados (canónicos) separados por coma", value="(ES),(IT),(DE),(FR),(UK)")
    markets_alias = st.text_area(
        "Aliases de mercados (opcional). Formato: CANON=alias1,alias2,... (1 por línea)",
        value="",
        height=120
    )
with cc2:
    tags_simple = st.text_input("Tipos/Tags de campaña separados por coma (internos)", value="NB,BR,AUTO")
    tags_alias = st.text_area(
        "Aliases de tags (opcional). Formato: CANON=alias1,alias2,... (1 por línea)",
        value="",
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

for df in (df_prev, df_curr):
    df["market"] = df["campaign_name"].apply(lambda x: detect_token(x, markets_map))
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
global_prev["cr"]   = safe_div(global_prev["orders"][0], global_prev["clicks"][0])
global_prev["cac"]  = safe_div(global_prev["spend"][0], global_prev["orders"][0])

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
global_curr["cr"]   = safe_div(global_curr["orders"][0], global_curr["clicks"][0])
global_curr["cac"]  = safe_div(global_curr["spend"][0], global_curr["orders"][0])

by_market_prev = aggregate(df_prev, ["market"])
by_market_curr = aggregate(df_curr, ["market"])
by_tag_prev    = aggregate(df_prev, ["camp_tag"])
by_tag_curr    = aggregate(df_curr, ["camp_tag"])
by_mkt_tag_prev = aggregate(df_prev, ["market","camp_tag"])
by_mkt_tag_curr = aggregate(df_curr, ["market","camp_tag"])

camp_prev = aggregate(df_prev, ["campaign_name","market","camp_tag"])
camp_curr = aggregate(df_curr, ["campaign_name","market","camp_tag"])
camp_mom  = add_mom(camp_prev, camp_curr, keys=["campaign_name","market","camp_tag"], label_prev="prev", label_curr="curr")

camp_mom_spend = camp_mom.sort_values("spend_delta", ascending=False)
camp_mom_sales = camp_mom.sort_values("sales_delta", ascending=False)

# =====================
# Client summary + email
# =====================
client_kpis = build_client_kpis(global_prev, global_curr)
client_insights = build_client_insights(
    global_prev=global_prev,
    global_curr=global_curr,
    by_market_prev=by_market_prev,
    by_market_curr=by_market_curr,
    camp_mom=camp_mom,
    period_prev_label=period_prev_label,
    period_curr_label=period_curr_label
)
client_actions = build_client_actions(global_prev, global_curr, camp_mom)

watchlist_df = pick_top_watchlist(camp_mom, top_n=3)
winners_df   = pick_top_winners(camp_mom, top_n=3)

# NUEVO: DF para priorización ACOS (útil en tab cliente + XLSX)
acos_priority_df = pick_top_acos_priorities(camp_mom, top_n=10, min_spend_curr=50.0)

client_insights_df = pd.DataFrame(client_insights)[["Title","What"]] if client_insights else pd.DataFrame(columns=["Title","What"])
client_actions_df = pd.DataFrame({"Acción": client_actions})

# =====================
# Email generator UI (TEXTO PLANO)
# =====================
st.divider()
st.subheader("📩 Email para el cliente (texto plano)")

e1, e2 = st.columns([1, 1])
with e1:
    sender_name = st.text_input("Tu nombre (firma)", value="Jordi")
with e2:
    st.caption("El email usa CLIENT + POC ya definidos arriba.")

email_subject, email_body = generate_client_email_es_plain(
    client=CLIENT,
    poc=POC,
    period_prev_label=period_prev_label,
    period_curr_label=period_curr_label,
    client_kpis=client_kpis,
    insights_list=client_insights,
    actions_list=client_actions,
    sender_name=sender_name
)

st.text_input("Asunto", value=email_subject)
st.text_area("Email (copia/pega)", value=email_body, height=380)

# =====================
# UI metrics quick
# =====================
st.subheader("✅ Preview del reporte (rápido)")

m1, m2, m3, m4 = st.columns(4)

spend_prev = float(global_prev["spend"][0]); spend_curr = float(global_curr["spend"][0])
sales_prev = float(global_prev["sales"][0]); sales_curr = float(global_curr["sales"][0])
orders_prev = int(global_prev["orders"][0]); orders_curr = int(global_curr["orders"][0])

acos_prev = float(global_prev["acos"][0]); acos_curr = float(global_curr["acos"][0])

total_spend_delta = spend_curr - spend_prev
total_sales_delta = sales_curr - sales_prev
orders_delta = orders_curr - orders_prev
acos_delta_pp = (acos_curr - acos_prev) * 100

m1.metric("Spend (Total)", f"{spend_curr:,.2f} €", f"{total_spend_delta:,.2f} €")
m2.metric("Sales (Total)", f"{sales_curr:,.2f} €", f"{total_sales_delta:,.2f} €")
m3.metric(
    "ACOS (Total)",
    f"{acos_curr*100:.2f} %",
    f"{acos_delta_pp:.2f} pp",
    delta_color="inverse"
)
m4.metric("Orders (Total)", f"{orders_curr:,}", f"{orders_delta:,}")

st.markdown("---")

markets_sorted = sorted(set(by_market_curr["market"]) | set(by_market_prev["market"]))
for mkt in markets_sorted:
    prev_row = by_market_prev[by_market_prev["market"] == mkt]
    curr_row = by_market_curr[by_market_curr["market"] == mkt]

    spend_p = float(prev_row["spend"].iloc[0]) if not prev_row.empty else 0.0
    sales_p = float(prev_row["sales"].iloc[0]) if not prev_row.empty else 0.0
    orders_p = int(prev_row["orders"].iloc[0]) if not prev_row.empty else 0

    spend_c = float(curr_row["spend"].iloc[0]) if not curr_row.empty else 0.0
    sales_c = float(curr_row["sales"].iloc[0]) if not curr_row.empty else 0.0
    orders_c = int(curr_row["orders"].iloc[0]) if not curr_row.empty else 0

    acos_p = safe_div(spend_p, sales_p)
    acos_c = safe_div(spend_c, sales_c)

    c1m, c2m, c3m, c4m = st.columns(4)
    c1m.metric(f"{mkt} · Spend", f"{spend_c:,.2f} €", f"{spend_c - spend_p:,.2f} €")
    c2m.metric(f"{mkt} · Sales", f"{sales_c:,.2f} €", f"{sales_c - sales_p:,.2f} €")
    c3m.metric(
        f"{mkt} · ACOS",
        f"{acos_c*100:.2f} %",
        f"{(acos_c - acos_p)*100:.2f} pp",
        delta_color="inverse"
    )
    c4m.metric(f"{mkt} · Orders", f"{orders_c:,}", f"{orders_c - orders_p:,}")

tabs = st.tabs(["Cliente (resumen)", "By Market", "By Tag", "Market x Tag", "Campaign MoM"])
with tabs[0]:
    st.caption("Versión reducida para cliente (genérica, accionable, sin entrar en profundidad).")
    st.markdown(f"**Cliente:** {CLIENT}  \n**Periodo:** {period_curr_label} vs {period_prev_label}")
    st.markdown("### KPIs clave (cliente)")
    st.dataframe(client_kpis[["KPI","Current","Previous","Delta","DeltaPct","Unit"]], use_container_width=True)

    st.markdown("### Insights (cliente)")
    st.dataframe(client_insights_df, use_container_width=True)

    st.markdown("### Próximas acciones")
    st.dataframe(client_actions_df, use_container_width=True)

    st.markdown("### Prioridades de optimización por ACOS (campañas)")
    st.caption("Top campañas con mayor variación de ACOS y peso de inversión (útil para priorizar ajustes por tipo NB/BR/AUTO).")
    st.dataframe(acos_priority_df, use_container_width=True)

    st.markdown("### Top campañas (rápido)")
    colA, colB = st.columns(2)
    with colA:
        st.caption("✅ Ganadoras (Sales ↑ y ACOS ↓/estable)")
        st.dataframe(winners_df, use_container_width=True)
    with colB:
        st.caption("⚠️ A revisar (Spend ↑ y Sales ↓)")
        st.dataframe(watchlist_df, use_container_width=True)

with tabs[1]:
    st.dataframe(add_mom(by_market_prev, by_market_curr, ["market"]), use_container_width=True)
with tabs[2]:
    st.dataframe(add_mom(by_tag_prev, by_tag_curr, ["camp_tag"]), use_container_width=True)
with tabs[3]:
    st.dataframe(add_mom(by_mkt_tag_prev, by_mkt_tag_curr, ["market","camp_tag"]), use_container_width=True)
with tabs[4]:
    st.caption("Ordenado por SpendΔ (desc).")
    st.dataframe(camp_mom_spend.head(50), use_container_width=True)

# =====================
# Export XLSX (sheet names con CLIENT)
# =====================
st.divider()
st.subheader("💾 Descargar XLSX")

output = BytesIO()
ts = datetime.now().strftime("%Y-%m-%d")
file_name = f"{CLIENT}_AmazonAds_Report_{period_curr_label.replace(' ','-')}_vs_{period_prev_label.replace(' ','-')}_{ts}.xlsx"

try:
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        client_kpis.to_excel(writer, index=False, sheet_name=sanitize_sheet_name(f"{client_prefix}_00_KPIs"))
        client_insights_df.to_excel(writer, index=False, sheet_name=sanitize_sheet_name(f"{client_prefix}_00_Insights"))
        client_actions_df.to_excel(writer, index=False, sheet_name=sanitize_sheet_name(f"{client_prefix}_00_Actions"))
        acos_priority_df.to_excel(writer, index=False, sheet_name=sanitize_sheet_name(f"{client_prefix}_00_ACOS_Prio"))
        winners_df.to_excel(writer, index=False, sheet_name=sanitize_sheet_name(f"{client_prefix}_00_Winners"))
        watchlist_df.to_excel(writer, index=False, sheet_name=sanitize_sheet_name(f"{client_prefix}_00_Watchlist"))

        gp = global_prev.copy(); gp.insert(0, "period", "prev")
        gc = global_curr.copy(); gc.insert(0, "period", "curr")
        global_sheet = pd.concat([gp, gc], ignore_index=True)
        global_sheet.to_excel(writer, index=False, sheet_name=sanitize_sheet_name(f"{client_prefix}_01_Global"))

        add_mom(by_market_prev, by_market_curr, ["market"]).to_excel(writer, index=False, sheet_name=sanitize_sheet_name(f"{client_prefix}_02_By_Market"))
        add_mom(by_tag_prev, by_tag_curr, ["camp_tag"]).to_excel(writer, index=False, sheet_name=sanitize_sheet_name(f"{client_prefix}_03_By_Tag"))
        add_mom(by_mkt_tag_prev, by_mkt_tag_curr, ["market","camp_tag"]).to_excel(writer, index=False, sheet_name=sanitize_sheet_name(f"{client_prefix}_04_Mkt_x_Tag"))

        camp_mom.to_excel(writer, index=False, sheet_name=sanitize_sheet_name(f"{client_prefix}_05_Campaign_MoM"))
        camp_mom_spend.head(100).to_excel(writer, index=False, sheet_name=sanitize_sheet_name(f"{client_prefix}_06_Top_Spend"))
        camp_mom_sales.head(100).to_excel(writer, index=False, sheet_name=sanitize_sheet_name(f"{client_prefix}_07_Top_Sales"))

except ModuleNotFoundError:
    st.error(
        "❌ No se puede generar el Excel porque falta la dependencia openpyxl.\n\n"
        "👉 Solución: añade openpyxl al requirements.txt o instálalo en tu entorno."
    )
    st.stop()

st.download_button(
    label="⬇️ Descargar Reporte XLSX",
    data=output.getvalue(),
    file_name=file_name,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True
)
