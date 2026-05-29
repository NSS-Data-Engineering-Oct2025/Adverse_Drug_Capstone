"""
FAERS vs Census Demographics Dashboard
Standalone Streamlit app — connect via .env (matches your Airflow DAG pattern)
Run: streamlit run faers_census_dashboard.py

Depends on three dbt models in marts/:
    - rpt_faers_age_distribution
    - rpt_faers_gender
    - rpt_census_demographics
"""

import plotly.graph_objects as go
import polars as pl
import snowflake.connector
import streamlit as st
from dotenv import dotenv_values

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="FAERS × Census",
    page_icon="⚕",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Mono:wght@300;400;500&display=swap');

html, body, [class*="css"] { font-family: 'DM Mono', monospace; }

.stApp { background-color: #0d0f14; color: #e8e4d9; }

section[data-testid="stSidebar"] {
    background-color: #131620;
    border-right: 1px solid #2a2f3e;
}
section[data-testid="stSidebar"] * { color: #e8e4d9 !important; }

.dash-title {
    font-family: 'DM Serif Display', serif;
    font-size: 2.6rem;
    letter-spacing: -0.02em;
    color: #e8e4d9;
    line-height: 1.1;
}
.dash-subtitle {
    font-family: 'DM Mono', monospace;
    font-size: 0.78rem;
    color: #5a6478;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    margin-top: 0.2rem;
}
.metric-card {
    background: #131620;
    border: 1px solid #2a2f3e;
    border-radius: 6px;
    padding: 1.1rem 1.4rem;
}
.metric-label {
    font-size: 0.68rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: #5a6478;
    margin-bottom: 0.3rem;
}
.metric-value {
    font-family: 'DM Serif Display', serif;
    font-size: 2rem;
    color: #c8f0a0;
    line-height: 1;
}
.metric-sub { font-size: 0.72rem; color: #5a6478; margin-top: 0.25rem; }
.section-header {
    font-family: 'DM Mono', monospace;
    font-size: 0.7rem;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: #5a6478;
    border-bottom: 1px solid #2a2f3e;
    padding-bottom: 0.4rem;
    margin: 1.5rem 0 1rem 0;
}
.js-plotly-plot { border-radius: 6px; }
hr { border-color: #2a2f3e; }
.stAlert { background-color: #131620; border-color: #2a2f3e; }
.filter-banner {
    background: #131620; border: 1px solid #2a2f3e;
    border-left: 3px solid #c8f0a0; border-radius: 4px;
    padding: 0.6rem 1rem; margin-bottom: 1rem;
    font-size: 0.72rem; color: #a0b0c0; line-height: 1.9;
}
.filter-banner strong { color: #e8e4d9; }
.filter-tag {
    display: inline-block; background: #1e2a1e; border: 1px solid #3a5a3a;
    border-radius: 3px; padding: 0.1rem 0.4rem; margin: 0.1rem 0.2rem;
    color: #c8f0a0; font-size: 0.68rem;
}
.filter-tag-census { background: #1a2535; border-color: #2a4060; color: #6ab0f5; }
</style>
""", unsafe_allow_html=True)

# ── Constants ──────────────────────────────────────────────────────────────────
CHART_COLORS = {
    "faers":  "#c8f0a0",
    "census": "#6ab0f5",
    "accent": "#f0a0c8",
}

GENDER_COLORS = {
    "M":   "#6ab0f5",  # blue
    "F":   "#f0a0c8",  # pink
    "UNK": "#c8f0a0",  # green (unknown/other)
}

PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Mono, monospace", color="#e8e4d9", size=11),
    margin=dict(l=10, r=10, t=30, b=10),
    legend=dict(
        bgcolor="rgba(13,15,20,0.8)",
        bordercolor="#2a2f3e",
        borderwidth=1,
        font=dict(color="#ffffff", size=11),
    ),
    xaxis=dict(gridcolor="#1e2330", zerolinecolor="#2a2f3e"),
    yaxis=dict(gridcolor="#1e2330", zerolinecolor="#2a2f3e"),
)

# Must match the age_group labels produced by rpt_faers_age_distribution
AGE_ORDER = [
    "Under 5", "5-9", "10-14", "15-19", "20-24", "25-34",
    "35-44", "45-54", "55-59", "60-64", "65-74", "75-84", "85+",
]

# Maps AGE_ORDER labels → census columns in rpt_census_demographics
AGE_TO_CENSUS_COL = {
    "Under 5": "POP_UNDER_5",
    "5-9":     "POP_5_TO_9",
    "10-14":   "POP_10_TO_14",
    "15-19":   "POP_15_TO_19",
    "20-24":   "POP_20_TO_24",
    "25-34":   "POP_25_TO_34",
    "35-44":   "POP_35_TO_44",
    "45-54":   "POP_45_TO_54",
    "55-59":   "POP_55_TO_59",
    "60-64":   "POP_60_TO_64",
    "65-74":   "POP_65_TO_74",
    "75-84":   "POP_75_TO_84",
    "85+":     "POP_85_PLUS",
}


# ── Snowflake helpers ──────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_connection(env_path: str) -> snowflake.connector.SnowflakeConnection:
    config = dotenv_values(env_path)
    missing = [k for k in ("SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT",
                            "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA")
                if not config.get(k)]
    if missing:
        raise ValueError(f".env loaded but missing keys: {missing}. Check the path is correct.")
    return snowflake.connector.connect(
        user=config["SNOWFLAKE_USER"],
        password=config["SNOWFLAKE_PASSWORD"],
        account=config["SNOWFLAKE_ACCOUNT"],
        warehouse=config["SNOWFLAKE_WAREHOUSE"],
        database=config["SNOWFLAKE_DATABASE"],
        schema="MARTS",
    )


@st.cache_data(show_spinner=False, ttl=3600)
def query(_conn: snowflake.connector.SnowflakeConnection, sql: str) -> pl.DataFrame:
    """Execute SQL and return a Polars DataFrame via Arrow (zero-copy path)."""
    cur = _conn.cursor()
    cur.execute(sql)
    arrow_table = cur.fetch_arrow_all()
    cur.close()
    if arrow_table is None:
        return pl.DataFrame()
    return pl.from_arrow(arrow_table)


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Configuration")
    env_path = st.text_input(
        ".env file path", value=".env",
        help="Use an absolute path if streamlit isn't launched from the same directory as your .env",
    )

    st.markdown("---")
    st.markdown("**Filters**")
    year_filter = st.multiselect(
        "Source Year (FAERS)",
        options=list(range(2004, 2026)),
        default=[],
        placeholder="All years",
    )
    gender_filter = st.multiselect(
        "Gender (FAERS)",
        options=["M", "F", "UNK"],
        default=[],
        placeholder="All genders",
    )

    connect_btn = st.button("🔌 Connect & Load", use_container_width=True, type="primary")
    if connect_btn:
        st.session_state.connected = True
    if st.button("🗑️ Clear cache", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.session_state.connected = False
        st.rerun()

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown('<div class="dash-title">FAERS × Census</div>', unsafe_allow_html=True)
st.markdown('<div class="dash-subtitle">Adverse event reporter demographics vs. U.S. population</div>', unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

if not st.session_state.connected:
    st.info("Configure your `.env` path in the sidebar and click **Connect & Load** to begin.")
    st.stop()

# ── Build FAERS WHERE clause (applied in Streamlit against pre-aggregated data) ─
# Filters are now post-query since the dbt models already group by year + gender
try:
    conn = get_connection(env_path)
except Exception as e:
    st.error(f"Snowflake connection failed: {e}")
    st.stop()

# ── Query the three dbt reporting models ──────────────────────────────────────
with st.spinner("Querying Snowflake…"):
    try:
        df_faers_age     = query(conn, "SELECT * FROM RPT_FAERS_AGE_DISTRIBUTION")
        df_faers_gender  = query(conn, "SELECT * FROM RPT_FAERS_GENDER")
        df_census        = query(conn, "SELECT * FROM RPT_CENSUS_DEMOGRAPHICS")
        df_ct            = query(conn, "SELECT * FROM RPT_CLINICAL_TRIALS")
    except Exception as e:
        st.error(f"Query failed: {e}")
        st.stop()

# Normalise column names to uppercase (Snowflake Arrow behaviour)
df_faers_age    = df_faers_age.rename({c: c.upper() for c in df_faers_age.columns})
df_faers_gender = df_faers_gender.rename({c: c.upper() for c in df_faers_gender.columns})
df_census       = df_census.rename({c: c.upper() for c in df_census.columns})
df_ct           = df_ct.rename({c: c.upper() for c in df_ct.columns})

# ── Drug & State filters (populated from data, rendered after load) ───────────────────
all_ingredients = sorted(df_faers_age["ACTIVE_INGREDIENT_NAME"].drop_nulls().unique().to_list())
all_brands      = sorted(df_faers_age["BRAND_NAME"].drop_nulls().unique().to_list())
all_states      = sorted(df_census["STATE_ABBREVIATION"].drop_nulls().unique().to_list())

with st.sidebar:
    st.markdown("---")
    st.markdown("**Drug filters**")
    ingredient_filter = st.multiselect(
        "Active ingredient",
        options=all_ingredients,
        default=[],
        placeholder="All ingredients",
    )
    brand_filter = st.multiselect(
        "Brand name",
        options=all_brands,
        default=[],
        placeholder="All brands",
    )
    state_filter = st.multiselect(
        "State (Census)",
        options=all_states,
        default=[],
        placeholder="All states",
    )

with st.sidebar:
    st.markdown("---")
    st.markdown("**Clinical Trials filters**")
    all_ct_status = sorted(df_ct["OVERALL_STATUS"].drop_nulls().unique().to_list())
    all_ct_phases = sorted(df_ct["PHASE_BUCKET"].drop_nulls().unique().to_list())
    ct_status_filter = st.multiselect(
        "Trial status", options=all_ct_status, default=[], placeholder="All statuses",
    )
    ct_phase_filter = st.multiselect(
        "Phase", options=all_ct_phases, default=[], placeholder="All phases",
    )

# ── Apply sidebar filters in Polars ───────────────────────────────────────────
if year_filter:
    df_faers_age    = df_faers_age.filter(pl.col("SOURCE_YEAR").is_in(year_filter))
    df_faers_gender = df_faers_gender.filter(pl.col("SOURCE_YEAR").is_in(year_filter))
if gender_filter:
    df_faers_age    = df_faers_age.filter(pl.col("GENDER").is_in(gender_filter))
    df_faers_gender = df_faers_gender.filter(pl.col("GENDER").is_in(gender_filter))
if ingredient_filter:
    df_faers_age    = df_faers_age.filter(pl.col("ACTIVE_INGREDIENT_NAME").is_in(ingredient_filter))
    df_faers_gender = df_faers_gender.filter(pl.col("ACTIVE_INGREDIENT_NAME").is_in(ingredient_filter))
if brand_filter:
    df_faers_age    = df_faers_age.filter(pl.col("BRAND_NAME").is_in(brand_filter))
    df_faers_gender = df_faers_gender.filter(pl.col("BRAND_NAME").is_in(brand_filter))
if state_filter:
    df_census       = df_census.filter(pl.col("STATE_ABBREVIATION").is_in(state_filter))

# Re-aggregate after filtering (models store one row per year+gender combination)
df_faers_age = (
    df_faers_age
    .group_by("AGE_GROUP")
    .agg(pl.col("REPORT_COUNT").sum())
)
df_faers_gender = (
    df_faers_gender
    .group_by("GENDER")
    .agg(pl.col("REPORT_COUNT").sum())
)
# Re-aggregate census across selected states (original model is one row per state)
if state_filter:
    census_pop_cols = ["MALE_POP", "FEMALE_POP", "TOTAL_POP", *list(AGE_TO_CENSUS_COL.values())]
    df_census = df_census.select([pl.sum(c).alias(c) for c in census_pop_cols])

# ── Apply CT filters & re-aggregate ──────────────────────────────────────────
if ct_status_filter:
    df_ct = df_ct.filter(pl.col("OVERALL_STATUS").is_in(ct_status_filter))
if ct_phase_filter:
    df_ct = df_ct.filter(pl.col("PHASE_BUCKET").is_in(ct_phase_filter))

df_ct_age    = df_ct.group_by("ELIGIBILITY_AGE_BUCKET").agg(pl.col("STUDY_COUNT").sum())
df_ct_gender = df_ct.group_by("ELIGIBLE_SEX").agg(pl.col("STUDY_COUNT").sum())
ct_total     = int(df_ct["STUDY_COUNT"].sum())

# ── Active filter banner ────────────────────────────────────────────────────────
active_filters = []
if year_filter:
    tags = " ".join(f'<span class="filter-tag">{y}</span>' for y in year_filter)
    active_filters.append(f"<strong>Year:</strong> {tags}")
if gender_filter:
    tags = " ".join(f'<span class="filter-tag">{g}</span>' for g in gender_filter)
    active_filters.append(f"<strong>Gender:</strong> {tags}")
if ingredient_filter:
    tags = " ".join(f'<span class="filter-tag">{i}</span>' for i in ingredient_filter)
    active_filters.append(f"<strong>Ingredient:</strong> {tags}")
if brand_filter:
    tags = " ".join(f'<span class="filter-tag">{b}</span>' for b in brand_filter)
    active_filters.append(f"<strong>Brand:</strong> {tags}")
if state_filter:
    tags = " ".join(f'<span class="filter-tag filter-tag-census">{s}</span>' for s in state_filter)
    active_filters.append(f"<strong>State:</strong> {tags}")
if ct_status_filter:
    tags = " ".join(f'<span class="filter-tag filter-tag-census">{s}</span>' for s in ct_status_filter)
    active_filters.append(f"<strong>Trial Status:</strong> {tags}")
if ct_phase_filter:
    tags = " ".join(f'<span class="filter-tag filter-tag-census">{p}</span>' for p in ct_phase_filter)
    active_filters.append(f"<strong>Phase:</strong> {tags}")
if active_filters:
    st.markdown(
        '<div class="filter-banner">🔍 &nbsp;' + " &nbsp;·&nbsp; ".join(active_filters) + "</div>",
        unsafe_allow_html=True,
    )

# ── KPI row ────────────────────────────────────────────────────────────────────
total_reports   = df_faers_age["REPORT_COUNT"].sum()
total_pop       = df_census["TOTAL_POP"][0] if df_census.height > 0 else 0
unique_age_grps = df_faers_age["AGE_GROUP"].n_unique()

col1, col2, col3, col4, col5 = st.columns(5)

def kpi(col, label, value, sub=""):
    col.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        <div class="metric-sub">{sub}</div>
    </div>""", unsafe_allow_html=True)

kpi(col1, "FAERS Reports",     f"{total_reports:,}",             "filtered")
kpi(col2, "Census Population", f"{total_pop / 1e6:.1f}M",        "total U.S.")
kpi(col3, "Age Groups",        str(unique_age_grps),             "with FAERS data")
kpi(col4, "Reporting Rate",
    f"{total_reports / total_pop * 100_000:.1f}" if total_pop else "—",
    "per 100k population")
kpi(col5, "Clinical Trials", f"{ct_total:,}", "filtered")

st.markdown("<br>", unsafe_allow_html=True)

# ── 01 Age distribution ────────────────────────────────────────────────────────
st.markdown('<div class="section-header">01 — Age Distribution</div>', unsafe_allow_html=True)

faers_age_total = df_faers_age["REPORT_COUNT"].sum()
df_faers_age_pct = df_faers_age.with_columns(
    (pl.col("REPORT_COUNT") / faers_age_total * 100).alias("FAERS_PCT")
)

# Build census age pcts from the single-row summary table
census_total = df_census["TOTAL_POP"][0]
census_age_rows = [
    {"AGE_GROUP": label, "CENSUS_PCT": df_census[col][0] / census_total * 100}
    for label, col in AGE_TO_CENSUS_COL.items()
]
df_census_age_pct = pl.DataFrame(census_age_rows)

# Join onto canonical age order spine
age_spine = pl.DataFrame({"AGE_GROUP": AGE_ORDER})
df_age_merged = (
    age_spine
    .join(df_faers_age_pct.select(["AGE_GROUP", "FAERS_PCT"]),  on="AGE_GROUP", how="left")
    .join(df_census_age_pct,                                     on="AGE_GROUP", how="left")
    .fill_null(0.0)
)

fig_age = go.Figure()
fig_age.add_trace(go.Bar(
    x=df_age_merged["AGE_GROUP"].to_list(),
    y=df_age_merged["CENSUS_PCT"].to_list(),
    name="Census population",
    marker_color=CHART_COLORS["census"],
    opacity=0.75,
))
fig_age.add_trace(go.Bar(
    x=df_age_merged["AGE_GROUP"].to_list(),
    y=df_age_merged["FAERS_PCT"].to_list(),
    name="FAERS reporters",
    marker_color=CHART_COLORS["faers"],
    opacity=0.9,
))
fig_age.update_layout(
    **PLOTLY_LAYOUT,
    barmode="group",
    yaxis_title="% of group",
    height=340,
    legend_orientation="h",
    legend_y=1.08,
    legend_x=0
)
st.plotly_chart(fig_age, use_container_width=True, key="fig_age")

# ── 02 Gender breakdown ────────────────────────────────────────────────────────
st.markdown('<div class="section-header">02 — Gender Breakdown</div>', unsafe_allow_html=True)

gcol1, gcol2 = st.columns(2)

_faers_gender_labels = df_faers_gender["GENDER"].to_list()
fig_faers_gender = go.Figure(go.Pie(
    labels=df_faers_gender["GENDER"].to_list(),
    values=df_faers_gender["REPORT_COUNT"].to_list(),
    hole=0.55,
    marker=dict(colors=[GENDER_COLORS.get(g, "#cccccc") for g in _faers_gender_labels]),
    textfont=dict(family="DM Mono, monospace", size=11),
))
fig_faers_gender.update_layout(
    **PLOTLY_LAYOUT,
    title=dict(text="FAERS Reporters", font=dict(size=12, color="#5a6478"), x=0.5),
    height=280,
)
gcol1.plotly_chart(fig_faers_gender, use_container_width=True, key="fig_faers_gender_02")

fig_census_gender = go.Figure(go.Pie(
    labels=["M", "F"],
    values=[df_census["MALE_POP"][0], df_census["FEMALE_POP"][0]],
    hole=0.55,
    marker=dict(colors=[GENDER_COLORS.get(g, "#cccccc") for g in ["M", "F"]]),
    textfont=dict(family="DM Mono, monospace", size=11),
))
fig_census_gender.update_layout(
    **PLOTLY_LAYOUT,
    title=dict(text="Census Population", font=dict(size=12, color="#5a6478"), x=0.5),
    height=280,
)
gcol2.plotly_chart(fig_census_gender, use_container_width=True, key="fig_census_gender_02")

# ── 03 Age comparison: FAERS · Census · Trial eligibility ─────────────────────
st.markdown('<div class="section-header">03 — Age Comparison: FAERS · Census · Trial Eligibility</div>', unsafe_allow_html=True)

CT_AGE_MAP = {
    "Includes children (<18)": ["Under 5", "5-9", "10-14", "15-19"],
    "Adults (18-64)":           ["20-24", "25-34", "35-44", "45-54", "55-59", "60-64"],
    "Older adults (65+)":       ["65-74", "75-84", "85+"],
}
CT_AGE_ORDER = ["Includes children (<18)", "Adults (18-64)", "Older adults (65+)"]

faers_bucket_counts = {
    b: df_faers_age_pct.filter(pl.col("AGE_GROUP").is_in(groups))["FAERS_PCT"].sum()
    for b, groups in CT_AGE_MAP.items()
}
faers_bucket_total = sum(faers_bucket_counts.values()) or 1

census_bucket_counts = {
    "Includes children (<18)": sum(df_census[c][0] for c in ["POP_UNDER_5","POP_5_TO_9","POP_10_TO_14","POP_15_TO_19"]) if df_census.height > 0 else 0,
    "Adults (18-64)":           sum(df_census[c][0] for c in ["POP_20_TO_24","POP_25_TO_34","POP_35_TO_44","POP_45_TO_54","POP_55_TO_59","POP_60_TO_64"]) if df_census.height > 0 else 0,
    "Older adults (65+)":       sum(df_census[c][0] for c in ["POP_65_TO_74","POP_75_TO_84","POP_85_PLUS"]) if df_census.height > 0 else 0,
}
census_bucket_total = sum(census_bucket_counts.values()) or 1

ct_bucket_counts = {
    b: int(df_ct_age.filter(pl.col("ELIGIBILITY_AGE_BUCKET") == b)["STUDY_COUNT"].sum())
    if df_ct_age.filter(pl.col("ELIGIBILITY_AGE_BUCKET") == b).height > 0 else 0
    for b in CT_AGE_ORDER
}
ct_bucket_total = sum(ct_bucket_counts.values()) or 1

fig_age_compare = go.Figure()
fig_age_compare.add_trace(go.Bar(
    x=CT_AGE_ORDER,
    y=[census_bucket_counts[b] / census_bucket_total * 100 for b in CT_AGE_ORDER],
    name="Census population",
    marker_color=CHART_COLORS["census"],
    opacity=0.75,
))
fig_age_compare.add_trace(go.Bar(
    x=CT_AGE_ORDER,
    y=[faers_bucket_counts[b] / faers_bucket_total * 100 for b in CT_AGE_ORDER],
    name="FAERS reporters",
    marker_color=CHART_COLORS["faers"],
    opacity=0.9,
))
fig_age_compare.add_trace(go.Bar(
    x=CT_AGE_ORDER,
    y=[ct_bucket_counts[b] / ct_bucket_total * 100 for b in CT_AGE_ORDER],
    name="Trial eligibility",
    marker_color=CHART_COLORS["accent"],
    opacity=0.9,
))
fig_age_compare.update_layout(
    **PLOTLY_LAYOUT,
    barmode="group",
    yaxis_title="% of group",
    height=340,
    legend_orientation="h",
    legend_y=1.08,
    legend_x=0,
)
st.plotly_chart(fig_age_compare, use_container_width=True, key="fig_age_compare")

# ── 04 Gender comparison: FAERS · Census · Trial eligibility ──────────────────
st.markdown('<div class="section-header">04 — Gender Comparison: FAERS · Census · Trial Eligibility</div>', unsafe_allow_html=True)

gcol_ct1, gcol_ct2, gcol_ct3 = st.columns(3)

fig_faers_g2 = go.Figure(go.Pie(
    labels=df_faers_gender["GENDER"].to_list(),
    values=df_faers_gender["REPORT_COUNT"].to_list(),
    hole=0.55,
    marker=dict(colors=[GENDER_COLORS.get(g, "#cccccc") for g in df_faers_gender["GENDER"].to_list()]),
    textfont=dict(family="DM Mono, monospace", size=11),
))
fig_faers_g2.update_layout(
    **PLOTLY_LAYOUT,
    title=dict(text="FAERS Reporters", font=dict(size=12, color="#5a6478"), x=0.5),
    height=280,
)
gcol_ct1.plotly_chart(fig_faers_g2, use_container_width=True, key="fig_faers_gender_04")

fig_census_g2 = go.Figure(go.Pie(
    labels=["M", "F"],
    values=[df_census["MALE_POP"][0], df_census["FEMALE_POP"][0]],
    hole=0.55,
    marker=dict(colors=[GENDER_COLORS.get(g, "#cccccc") for g in ["M", "F"]]),
    textfont=dict(family="DM Mono, monospace", size=11),
))
fig_census_g2.update_layout(
    **PLOTLY_LAYOUT,
    title=dict(text="Census Population", font=dict(size=12, color="#5a6478"), x=0.5),
    height=280,
)
gcol_ct2.plotly_chart(fig_census_g2, use_container_width=True, key="fig_census_gender_04")

CT_SEX_LABEL = {"ALL": "All", "FEMALE": "F", "MALE": "M"}
ct_gender_labels = [CT_SEX_LABEL.get(s, s) for s in df_ct_gender["ELIGIBLE_SEX"].to_list()]
fig_ct_gender = go.Figure(go.Pie(
    labels=ct_gender_labels,
    values=df_ct_gender["STUDY_COUNT"].to_list(),
    hole=0.55,
    marker=dict(colors=[GENDER_COLORS.get(l, "#cccccc") for l in ct_gender_labels]),
    textfont=dict(family="DM Mono, monospace", size=11),
))
fig_ct_gender.update_layout(
    **PLOTLY_LAYOUT,
    title=dict(text="Trial Eligibility (sex)", font=dict(size=12, color="#5a6478"), x=0.5),
    height=280,
)
gcol_ct3.plotly_chart(fig_ct_gender, use_container_width=True, key="fig_ct_gender_04")

# ── Raw data expanders ─────────────────────────────────────────────────────────
with st.expander("Raw data — FAERS age"):
    st.dataframe(df_faers_age, use_container_width=True)
with st.expander("Raw data — Census demographics"):
    st.dataframe(df_census, use_container_width=True)
with st.expander("Raw data — Clinical Trials"):
    st.dataframe(df_ct, use_container_width=True)

# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    '<span style="font-size:0.7rem;color:#5a6478;letter-spacing:0.1em;">'
    'DATA — FDA FAERS + U.S. Census Bureau &nbsp;·&nbsp; '
    'Built with Streamlit + Polars + Plotly &nbsp;·&nbsp; '
    'Queries cached 1 hr</span>',
    unsafe_allow_html=True,
)
