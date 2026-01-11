import streamlit as st
import awswrangler as wr
import pandas as pd
import plotly.express as px

st.set_page_config(layout="wide", page_title="NH Healthcare Dashboard")

@st.cache_data
def load_table(table, database="nh_silver"):
  df = wr.s3.read_parquet_table(database=database, table=table)
  return df

@st.cache_data
def get_analytics_df():
  dates = load_table("dim_dates")[["date","day_of_week","day_of_week_name","is_weekday"]]
  staff = load_table("fct_staffing_levels")[["provider_id","work_date","num_patients","hrs_registered_nurses"]]
  prov = load_table("dim_providers")[["provider_id","provider_name","provider_state","ownership_type","num_certified_beds", "substantiated_complaints", "rn_turnover", "qm_rating"]].sample(400)
  df = (staff
        .merge(prov, on="provider_id")
        .merge(dates, left_on="work_date", right_on="date")
        .drop(columns=["work_date"])
  )
  df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
  df = df.dropna(subset=["date", "provider_id"])
  df["occupancy_rate"] = df["num_patients"] / df["num_certified_beds"]
  df["hrs_per_res"] = df["hrs_registered_nurses"] / df["num_patients"]
  return df

@st.cache_data
def filter_df(df, start, end, state=None, providers=None):
  df = df[(df["date"] >= start) & (df["date"] <= end)]
  # df = df[(df["is_weekday"] == True)]
  if state: df = df[df["provider_state"].isin(state)]
  if providers: df = df[df["provider_id"].isin(providers)]
  return df


# ---- Load dataframe
df = get_analytics_df()

# ---- Sidebar filters
st.sidebar.header("Filters")
min_date, max_date = df["date"].min(), df["date"].max()
start, end = st.sidebar.date_input("Date range", value=(min_date, max_date))
state = st.sidebar.multiselect("State", sorted(df["provider_state"].dropna().unique()), default=[])
providers = st.sidebar.multiselect("Provider(s)", sorted(df["provider_id"].unique()), default=[])

# ---- Apply filters to dataframe
f = filter_df(df=df, start=start, end=end, state=state, providers=providers)


# AGG FOR TIME-RELATED PLOTS at daily level (across all providers)
daily = (f.groupby(["date", "day_of_week", "day_of_week_name", "is_weekday"], as_index=False)
         .agg(num_patients=("num_patients","sum"),
              occupancy_rate=("occupancy_rate","mean"),
              rn_hours=("hrs_registered_nurses","sum")
          ))
daily["hrs_per_res"] = daily["rn_hours"] / daily["num_patients"]
daily["occ_7d"] = daily["occupancy_rate"].rolling(7).mean()
daily["hpr_7d"] = daily["hrs_per_res"].rolling(7).mean()

# ---- KPIs (based on agg daily metrics across all providers)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Residents",              f"{int(daily['num_patients'].sum()):,}")
c2.metric("Total Staffing Hours (RN)",               f"{float(daily['rn_hours'].sum()):,.1f}")
c3.metric("Hours Per Resident", f"{float(daily['hrs_per_res'].mean()):.3f}")
c4.metric("Occupancy Rate",   f"{float(daily['occupancy_rate'].mean()):.3f}")

st.divider()

# ---- Charts


left, right = st.columns(2)
# (Multi-line chart) avg occupancy rate over time (daily line + 7d rolling line)
with left:
    metric_name_1 = st.selectbox("Metric over time", ["Occupancy Rate", "RN Hrs Per Resident"])
    if metric_name_1 == "RN Hrs Per Resident": 
      daily_metric_1 = "hrs_per_res"
      roll7 = "hpr_7d"
    else: 
      daily_metric_1 = "occupancy_rate"
      roll7 = "occ_7d"

    fig = px.line(daily, x="date", y=daily_metric_1, title=f"{metric_name_1} over time")
    fig.add_scatter(x=daily["date"], y=daily[roll7], name="7d rolling", mode="lines")
    st.plotly_chart(fig, use_container_width=True)
# (Multi-box chart) hrs per resident (registered nurses) by day of week
with right:
    metric_name_2 = st.selectbox("Metric", ["RN Hrs Per Resident", "Occupancy Rate"])
    if metric_name_2 == "RN Hrs Per Resident": daily_metric_2 = "hrs_per_res"
    else: daily_metric_2 = "occupancy_rate"    
    fig = px.box(daily, x="day_of_week_name", y=daily_metric_2, title=f"{metric_name_2} by day of week")
    st.plotly_chart(fig, use_container_width=True)



# AGG FOR SCATTER PLOTS at provider-level (less noisy than provider-day level)
prov_agg = (f.groupby(["provider_id", "provider_state"], as_index=False)
            .agg(avg_num_patients=("num_patients","mean"),
                  total_rn_hours=("hrs_registered_nurses","sum"),
                  avg_hrs_per_res=("hrs_per_res","mean"),
                  avg_occupancy_rate=("occupancy_rate","mean"),
                  qm_rating=("qm_rating","mean"),num_complaints=("substantiated_complaints","mean"),nurse_attrition_rate=("rn_turnover","mean")))

col1, col2, col3 = st.columns(3)
# (Scatter plot) avg hrs per resident vs. nurse attrition rate
with col1:
  fig = px.scatter(
      prov_agg, x="avg_hrs_per_res", y="nurse_attrition_rate",
      size="avg_num_patients",
      hover_data=["provider_id","avg_num_patients"],
      title="RN hours per resident vs nurse attrition rate"
  )
  st.plotly_chart(fig, use_container_width=True)

# (Scatter plot) avg hrs per resident vs. num complaints
with col2:
  fig = px.scatter(
      prov_agg, x="avg_hrs_per_res", y="num_complaints",
      size="avg_num_patients",
      hover_data=["provider_id","avg_num_patients"],
      title="RN hours per resident vs num compliants"
  )
  st.plotly_chart(fig, use_container_width=True)

# (Scatter plot) avg hrs per resident vs. qm_rating
with col3:
  prov_agg["avg_hrs_per_res"] = prov_agg["avg_hrs_per_res"].clip(upper=1.3)
  fig = px.box(
      prov_agg, y="avg_hrs_per_res", x="qm_rating",
      # size="avg_num_patients",
      hover_data=["provider_id","avg_num_patients"],
      title="RN hours per resident vs quality rating"
  )
  st.plotly_chart(fig, use_container_width=True)  






bottom_left, bottom_right = st.columns(2)
with bottom_left:
  fig = px.bar(f, x="ownership_type", y="hrs_per_res", title="Hours per res by ownership type")
  st.plotly_chart(fig, use_container_width=True)

with bottom_right:    
  state_agg = (prov_agg.groupby("provider_state", as_index=False)
            .agg(avg_num_patients=("avg_num_patients","mean"),
                  total_rn_hours=("total_rn_hours","sum"),
                  avg_hrs_per_res=("avg_hrs_per_res","mean"),
                  avg_occupancy_rate=("avg_occupancy_rate","mean"))
            .sort_values("avg_hrs_per_res", ascending=True)
                  )


  
  
  fig = px.bar(state_agg, x="provider_state", y="avg_hrs_per_res", title="Avg hours per res by state")
  st.plotly_chart(fig, use_container_width=True)



