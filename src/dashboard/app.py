import streamlit as st
import awswrangler as wr
import plotly.express as px

@st.cache_data
def load_table(database: str, table: str):
  df = wr.s3.read_parquet_table(database=database, table=table)
  return df

@st.cache_data
def load_staffing_levels():
  return load_table("nh_silver", "fct_staffing_levels")

@st.cache_data
def load_providers():
  return load_table("nh_silver", "dim_providers")

st.set_page_config(
    layout="wide",
    page_title="NH Healthcare Dashboard PAGE TITLE",  # optional
    initial_sidebar_state="expanded"       # optional
)

staffing_levelsDF = load_table("nh_silver", "fct_staffing_levels")
providersDF = load_table("nh_silver", "dim_providers")
claimsDF = load_table("nh_silver", "dim_claims")
datesDF = load_table("nh_silver", "dim_dates")


# Sidebar filters
st.sidebar.header("Filters")
year = st.sidebar.selectbox("Select City", sorted(staffing_levelsDF['provider_city'].unique()))
state = st.sidebar.multiselect("Select State", staffing_levelsDF['provider_state'].unique(), default=staffing_levelsDF['provider_state'].unique())

filtered_staffing_levelsDF = staffing_levelsDF[(staffing_levelsDF['provider_state'].isin(state))]


# Metrics

total_patients = int(filtered_staffing_levelsDF["num_patients"].sum())
total_rn_hours = int(filtered_staffing_levelsDF["hrs_registered_nurses"].sum())
total_providers = filtered_staffing_levelsDF["provider_id"].nunique()


st.title("NH Healthcare Dashboard")
kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric("Total Patients", f"{total_patients:,}")
kpi2.metric("Total Registered Nurse Staffing Hours", f"{total_rn_hours:,}")
kpi3.metric("Unique Providers", total_providers)


st.markdown("---")

# Charts

col1, col2 = st.columns([2, 1])

with col1:    
    plot1DF = filtered_staffing_levelsDF.groupby("work_date").sum("num_patients").reset_index().sort_values("work_date")
    fig = px.line(plot1DF, x="work_date", y="num_patients", title="Patient Load Trend")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    plot2DF = filtered_staffing_levelsDF.groupby("work_date").sum("hrs_registered_nurses").reset_index().sort_values("work_date")
    fig = px.line(plot2DF, x="work_date", y="hrs_registered_nurses", title="Hours Registered Nurses")
    st.plotly_chart(fig, use_container_width=True)

plot3DF = filtered_staffing_levelsDF.groupby("work_date").mean("hrs_registered_nurses", "num_patients").reset_index().sort_values("work_date")
st.line_chart(plot3DF, x="work_date", y="hrs_registered_nurses", x_label="Date", y_label="Hours Registered Nurses", width="stretch")


claimsDF
datesDF