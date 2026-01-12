from config import *


def load_bronze_table(spark, table, start_date):
  df = spark.table(table)
  df = df.filter(F.col("ingest_date") >= start_date) 
  info(f"Reading from table: {table}\n{df.count()} new records after start_date: {start_date}")
  return df

def main_silver(spark, job_name, pipeline_mode, start_date):

  c = load_config(job_name="SILVER JOB")
  info(f"Starting job {c.job_name} in {c.env} environment.")

  silver_bronze = {
    "dim_claims":           "claims_raw",
    "dim_providers":        "providers_raw",
    "fct_staffing_levels":  "daily_staffing_raw"
  }
  silver_functions = {
    "dim_claims":           clean_claims,
    "dim_providers":        clean_providers,
    "fct_staffing_levels":  clean_staffing_levels
  }

  NO_START_DATE = "1900-01-01"
  if pipeline_mode == "full": 
    start_date = NO_START_DATE

  for silver_table, bronze_table in silver_bronze.items():
    

    if silver_table == "fct_staffing_levels":
      inputDF = load_bronze_table(spark, f"{c.bronze_fqn}.{bronze_table}", start_date)
      cleanDF = silver_functions[silver_table](inputDF).withColumn("updated_at", F.current_timestamp())
      overwrite_partitions(spark, cleanDF, table=f"{c.silver_db}.{silver_table}", partition_col="ingest_date")
    else:
      inputDF = load_bronze_table(spark, f"{c.bronze_fqn}.{bronze_table}", NO_START_DATE)
      cleanDF = silver_functions[silver_table](inputDF).withColumn("updated_at", F.current_timestamp())
      overwrite_table(cleanDF, table=f"{c.silver_db}.{silver_table}")


  dim_datesDF = generate_dim_dates(spark, start_date="2024-04-01", end_date="2024-06-30").withColumn("updated_at", F.current_timestamp())
  overwrite_table(dim_datesDF, table=f"{c.silver_db}.dim_dates")

  table_counts(spark, schemas=[c.bronze_fqn, c.silver_fqn])
  info(f"Completed job: {c.job_name}.")