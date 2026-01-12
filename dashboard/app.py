import streamlit as st
import awswrangler as wr
import pandas as pd
import plotly.express as px
import os

st.set_page_config(layout="wide", page_title="NH Healthcare Dashboard")

# -------------------------
# Data helpers
# -------------------------

wr.config.workgroup = "personal-dev"

@st.cache_data
def query_athena(sql, database="nh_gold"):
    return wr.athena.read_sql_query(sql=sql, database=database)

@st.cache_data
def get_filter_values():
    dates = query_athena("""
        SELECT MIN(date) AS min_date, MAX(date) AS max_date
        FROM nh_gold.agg_daily_metrics
    """)
    min_date = dates.loc[0, "min_date"]
    max_date = dates.loc[0, "max_date"]
    
    states_list = query_athena("""
        SELECT DISTINCT state
        FROM nh_gold.agg_state_metrics
        WHERE state IS NOT NULL
        ORDER BY state
    """)["state"].tolist()
    
    providers_list = query_athena("""
        SELECT DISTINCT provider_id
        FROM nh_gold.agg_provider_metrics
        WHERE provider_id IS NOT NULL
        ORDER BY provider_id
    """)["provider_id"].tolist()

    return min_date, max_date, states_list, providers_list

# -------------------------
# UI helpers
# -------------------------

def kpi_row(*kpi_pairs, divider=True):
    "Creates a row of KPIs based on (kpi name, kpi value) pairs (kpi value can be string or number)"
    sections = st.columns(len(kpi_pairs))
    for s, (label, value) in zip(sections, kpi_pairs):
        s.metric(label, value)
    if divider: st.divider()

def row(*fn_list):
  length = len(fn_list)
  for c, fn in zip(st.columns(length), fn_list):
    with c:
      fn()

def padded_range(vals, pad=0.2):
  min = vals.min() # use numpy in case of multiple cols
  max = vals.max()
  pad = (max - min) * 0.2
  return min - pad, max + pad

# -------------------------
# Sidebar filters
# -------------------------

min_date, max_date, states_list, providers_list = get_filter_values()
st.sidebar.header("Filters")

start, end = st.sidebar.date_input("Date range", value=(min_date, max_date))
weekdays_only = st.sidebar.toggle("Weekdays only", value=False)
states = st.sidebar.multiselect("States", sorted(states_list), default=[])
providers = st.sidebar.multiselect("Providers", sorted(providers_list), default=[])

# -------------------------
# Load filtered data
# -------------------------
def build_where(start=None, end=None, weekdays_only=False, states=None, providers=None):
    parts = []
    if start and end: parts.append(f"date BETWEEN DATE '{start.isoformat()}' AND DATE '{end.isoformat()}'")
    if weekdays_only: parts.append("is_weekday = true")
    if states:        
      states_sql = ", ".join(f"'{s}'" for s in states)
      parts.append(f"state IN ({states_sql})")
    if providers:     
      providers_sql = ", ".join(f"'{p}'" for p in providers)
      parts.append(f"provider_id IN ({providers_sql})")
    return ("WHERE " + " AND ".join(parts)) if parts else ""  

def load_agg_daily():
  where = build_where(start=start, end=end, weekdays_only=weekdays_only, states=states, providers=providers)
  agg_daily_sql = f"""
WITH daily AS (
  SELECT
    date,
    day_of_week,
    day_of_week_name,
    is_weekday,
    SUM(num_patients)                             AS num_patients,
    AVG(occupancy_rate)                           AS occupancy_rate,
    SUM(rn_hrs)                                   AS rn_hours,
    (SUM(rn_hrs) / NULLIF(SUM(num_patients), 0))  AS hrs_per_res
  FROM daily_provider_metrics
  {where}
  GROUP BY 1,2,3,4
)
SELECT
  *,
  AVG(occupancy_rate) OVER (
    ORDER BY date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS occ_7d,
  AVG(hrs_per_res) OVER (
    ORDER BY date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS hpr_7d
FROM daily
ORDER BY date
  """
  # st.write(agg_daily_sql)
  return query_athena(agg_daily_sql)
  
def load_agg_prov():
  where = build_where(start=start, end=end, weekdays_only=weekdays_only, states=states, providers=providers)
  agg_prov_sql = f"""
WITH base AS (
  SELECT * FROM daily_provider_metrics
  {where}
)
SELECT
  provider_id,
  provider_name,
  state,
  ownership_type,

  AVG(num_patients)           AS avg_num_patients,
  SUM(rn_hrs)                 AS total_rn_hours,
  AVG(hrs_per_res)            AS avg_hrs_per_res,
  AVG(occupancy_rate)         AS avg_occupancy_rate,

  MAX(num_beds)               AS num_beds,
  MAX(readmission_rate)       AS readmission_rate,
  MAX(num_complaints)         AS num_complaints,
  MAX(rn_turnover)            AS rn_turnover,
  MAX(qm_rating)              AS qm_rating
FROM base
GROUP BY 1, 2, 3, 4
"""
  # st.write(agg_prov_sql)
  return query_athena(agg_prov_sql)

def load_state_prov():
  where = build_where(start=start, end=end, weekdays_only=weekdays_only, states=states, providers=providers)
  agg_state_sql = f"""
WITH base AS (
  SELECT * FROM daily_provider_metrics
  {where}
)
SELECT
  state,
  AVG(num_patients)           AS avg_num_patients,
  SUM(rn_hrs)                 AS total_rn_hours,
  SUM(num_beds)               AS total_beds,
  AVG(hrs_per_res)            AS avg_hrs_per_res,
  AVG(occupancy_rate)         AS avg_occupancy_rate,
  AVG(qm_rating)              AS qm_rating,
  AVG(num_complaints)         AS num_complaints,
  AVG(rn_turnover)            AS rn_turnover
FROM base
GROUP BY 1
  """
  # st.write(agg_state_sql)
  return query_athena(agg_state_sql)    

daily = load_agg_daily()
prov_agg = load_agg_prov()
state_agg = load_state_prov()

# -------------------------
# KPIs (daily metrics agg across providers)
# -------------------------
kpi_row(
    ("Total Residents",           f"{int(daily['num_patients'].sum()):,}"),
    ("Total Staffing Hours (RN)", f"{float(daily['rn_hours'].sum()):,.1f}"),
    ("Hours Per Resident",        f"{float(daily['hrs_per_res'].mean()):.3f}"),
    ("Occupancy Rate",            f"{float(daily['occupancy_rate'].mean()):.3f}")
)
# -------------------------
# Charts: daily trends
# -------------------------

LINE_METRICS = {
  "Occupancy rate":        ["occupancy_rate", "occ_7d"],
  "RN hours per resident":   ["hrs_per_res", "hpr_7d"]}

BOX_METRICS = {
  "RN hours per resident":   "hrs_per_res",
  "Occupancy rate":        "occupancy_rate"}

def select_metric_line_plot():
  label = st.selectbox("Metric over time", LINE_METRICS.keys())
  base, roll7d = LINE_METRICS[label]

  fig = px.line(daily, x="date", y=[base, roll7d], title=f"{label} over time")
  y0, y1 = padded_range(daily[base])
  fig.update_yaxes(range=[y0, y1])
  st.plotly_chart(fig, use_container_width=True)

def select_metric_box_plot():  
  label = st.selectbox("Metric", BOX_METRICS.keys())
  ycol = BOX_METRICS[label]
  
  fig = px.box(daily, x="day_of_week_name", y=ycol, title=f"{label} by day of week")
  st.plotly_chart(fig, use_container_width=True)

row(select_metric_line_plot, select_metric_box_plot)

# -------------------------
# Charts: provider-level
# -------------------------

def scatter_complaints():
  fig = px.scatter(
      prov_agg, x="avg_hrs_per_res", y="num_complaints",
      size="avg_num_patients",
      hover_data=["provider_id","avg_num_patients"],
      title="Num complaints vs. hours per resident"
  )
  st.plotly_chart(fig, use_container_width=True)

def box_qm_rating():
  df = prov_agg.copy()
  df["avg_hrs_per_res"] = df["avg_hrs_per_res"].clip(upper=1.3)
  fig = px.box(
      df, y="avg_hrs_per_res", x="qm_rating",
      hover_data=["provider_id","avg_num_patients"],
      title="Quality ratings vs. hours per resident"
  )
  st.plotly_chart(fig, use_container_width=True)  

def scatter_readmission():
  fig = px.scatter(
      prov_agg, x="avg_hrs_per_res", y="readmission_rate",
      size="avg_num_patients",
      hover_data=["provider_id","avg_num_patients"],
      title="Readmission rates vs. hours per resident"
  )
  st.plotly_chart(fig, use_container_width=True)

def scatter_attrition():
  fig = px.scatter(
      prov_agg, x="avg_hrs_per_res", y="rn_turnover",
      size="avg_num_patients",
      hover_data=["provider_id","avg_num_patients"],
      title="Nurse attrition rates vs. hours per resident"
  )
  st.plotly_chart(fig, use_container_width=True)

row(scatter_complaints, box_qm_rating, scatter_readmission, scatter_attrition)

# ----------------
# Chart: agg bar charts (hpr by ownership type, state)
# ----------------

def ownership_bar_hpr():
  fig = px.bar(prov_agg, x="ownership_type", y="avg_hrs_per_res", title="Hours per resident by ownership type")
  fig.update_xaxes(categoryorder="total descending")
  st.plotly_chart(fig, use_container_width=True)

def state_bar_hpr():
  fig = px.bar(state_agg, x="state", y="avg_hrs_per_res", title="Avg hours per resident by state")
  fig.update_xaxes(categoryorder="total descending")
  st.plotly_chart(fig, use_container_width=True)
      

row(ownership_bar_hpr, state_bar_hpr)
  
# ----------------
# Chart: top providers ranked by metric
# ----------------  

def top_hbar(df, metric, by, n=5, title_metric=None, title_by=None, largest=True, hover_data=None, omit_zero=True):
  df = df.copy()
  if omit_zero: df = df[df[metric].notna() & (df[metric] != 0)]
  else: df = df[df[metric].notna()]
  pick = df.nlargest(n, metric) if largest else df.nsmallest(n, metric)

  top = pick.sort_values(metric, ascending=True)
  title_metric = title_metric or metric
  title_by = title_by or by
  title_order = "Top" if largest else "Bottom"
  fig = px.bar(top, x=metric, y=by, orientation="h", title=f"{title_order} {n} {title_by} by {title_metric}", hover_data=hover_data)
  st.plotly_chart(fig, use_container_width=True)

def top_prov_by_hours():
  top_hbar(prov_agg,
    metric="total_rn_hours", by="provider_name", n=10, 
    hover_data=["avg_num_patients"],
    title_metric="total RN hours", title_by="providers"
  )
def top_prov_by_hpr():  
  top_hbar(prov_agg,
    metric="avg_hrs_per_res", by="provider_name", n=10, 
    hover_data=["avg_num_patients"],
    title_metric="hours per resident", title_by="providers"
  )  
def low_prov_by_hpr():  
  top_hbar(prov_agg,
    metric="avg_hrs_per_res", by="provider_name", n=10, 
    hover_data=["avg_num_patients"],
    title_metric="hours Per resident", title_by="providers",
    largest=False
  )    
row(top_prov_by_hours, top_prov_by_hpr, low_prov_by_hpr)
