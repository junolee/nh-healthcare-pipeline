import streamlit as st
import awswrangler as wr
import pandas as pd
import plotly.express as px

st.set_page_config(layout="wide", page_title="NH Healthcare Dashboard")

@st.cache_data
def load_table(table, database="nh_gold"):
  df = wr.s3.read_parquet_table(database=database, table=table)
  return df

@st.cache_data
def filter_df(df, start=None, end=None, weekdays_only=None, states=None, providers=None):
  if start: df = df[(df["date"] >= start) & (df["date"] <= end)]
  if weekdays_only: df = df[df["is_weekday"] == True]
  if states: df = df[df["state"].isin(states)]
  if providers: df = df[df["provider_id"].isin(providers)]
  return df

# ---- Load data
agg_daily = load_table("agg_daily_metrics")
agg_prov = load_table("agg_provider_metrics")
agg_state = load_table("agg_state_metrics")



# ---- Sidebar filters
st.sidebar.header("Filters")

min_date, max_date = agg_daily["date"].min(), agg_daily["date"].max()
start, end = st.sidebar.date_input("Date range", value=(min_date, max_date))
states = st.sidebar.multiselect("State(s)", sorted(agg_state["state"].dropna().unique()), default=[])
providers = st.sidebar.multiselect("Provider(s)", sorted(agg_prov["provider_id"].unique()), default=[])
weekdays_only = st.sidebar.toggle("Weekdays only", value=False)

# ---- Apply filters to dataframe

daily = filter_df(df=agg_daily, start=start, end=end, weekdays_only=weekdays_only)
prov_agg = filter_df(df=agg_prov, states=states, providers=providers)
state_agg = filter_df(df=agg_state, states=states)



def create_kpi_row(*kpi_pairs, divider=True):
    "Creates a row of KPIs based on (kpi name, kpi value) pairs (kpi value can be string or number)"
    sections = st.columns(len(kpi_pairs))
    for s, (label, value) in zip(sections, kpi_pairs):
        s.metric(label, value)
    if divider: st.divider()

# ---- KPIs (based on agg daily metrics across all providers)
create_kpi_row(
    ("Total Residents",           f"{int(daily['num_patients'].sum()):,}"),
    ("Total Staffing Hours (RN)", f"{float(daily['rn_hours'].sum()):,.1f}"),
    ("Hours Per Resident",        f"{float(daily['hrs_per_res'].mean()):.3f}"),
    ("Occupancy Rate",            f"{float(daily['occupancy_rate'].mean()):.3f}")
)


# ----------------
# CHARTS
# ----------------


def scale_range(vals, pad=0.2):
  min = vals.min() # use numpy in case of multiple cols
  max = vals.max()
  pad = (max - min) * 0.2
  return min - pad, max + pad


def create_row(fn_list):
  length = len(fn_list)
  for c, fn in zip(st.columns(length), fn_list):
    with c:
      fn()


def select_metric_line_plot():
  LINE_X_COLUMN = "date"
  LINE_Y_COLUMNS = {
    "Occupancy Rate":        ["occupancy_rate", "occ_7d"],
    "RN Hrs Per Resident":   ["hrs_per_res", "hpr_7d"]}
  
  line_metric = st.selectbox("Metric over time", LINE_Y_COLUMNS.keys())
  line_ycols = LINE_Y_COLUMNS[line_metric]

  fig = px.line(daily, x=LINE_X_COLUMN, y=line_ycols, title=f"{line_metric} over time")
  min, max = scale_range(daily[line_ycols[0]])
  fig.update_yaxes(range=[min, max])
  st.plotly_chart(fig, use_container_width=True)


def select_metric_box_plot():
  BOX_X_COLUMN = "day_of_week_name"
  BOX_Y_COLUMNS = {
    "RN Hrs Per Resident":   "hrs_per_res",
    "Occupancy Rate":        "occupancy_rate"}
  
  box_metric = st.selectbox("Metric", BOX_Y_COLUMNS.keys())
  box_ycol = BOX_Y_COLUMNS[box_metric]
  
  fig = px.box(daily, x=BOX_X_COLUMN, y=box_ycol, title=f"{box_metric} by day of week")
  st.plotly_chart(fig, use_container_width=True)






create_row([select_metric_line_plot, select_metric_box_plot])






# ----------------
# AGG_PROV --> CHARTS
# ----------------

def scatter_attrition():
  fig = px.scatter(
      prov_agg, x="avg_hrs_per_res", y="rn_turnover",
      size="avg_num_patients",
      hover_data=["provider_id","avg_num_patients"],
      title="RN hours per resident vs nurse attrition rate"
  )
  st.plotly_chart(fig, use_container_width=True)

def scatter_complaints():
  fig = px.scatter(
      prov_agg, x="avg_hrs_per_res", y="num_complaints",
      size="avg_num_patients",
      hover_data=["provider_id","avg_num_patients"],
      title="RN hours per resident vs num compliants"
  )
  st.plotly_chart(fig, use_container_width=True)

def scatter_qm_rating():
  prov_agg["avg_hrs_per_res"] = prov_agg["avg_hrs_per_res"].clip(upper=1.3)
  fig = px.box(
      prov_agg, y="avg_hrs_per_res", x="qm_rating",
      # size="avg_num_patients",
      hover_data=["provider_id","avg_num_patients"],
      title="RN hours per resident vs quality rating"
  )
  st.plotly_chart(fig, use_container_width=True)  


create_row([scatter_attrition, scatter_complaints, scatter_qm_rating])




# ----------------
# AGG_PROV - CHART
# ----------------
bottom_left, bottom_right = st.columns(2)
with bottom_left:
  fig = px.bar(prov_agg, x="ownership_type", y="avg_hrs_per_res", title="Hours per res by ownership type")
  st.plotly_chart(fig, use_container_width=True)

# ----------------
# AGG_STATE - CHART
# ----------------

with bottom_right:      
  fig = px.bar(state_agg, x="state", y="avg_hrs_per_res", title="Avg hours per res by state")
  st.plotly_chart(fig, use_container_width=True)



