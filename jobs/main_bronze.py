from config import *
import pyspark.sql.functions as F


def load_raw_input(spark, path, start_date, sample):
  df = load_csv(spark, path).filter(F.col("ingest_date") >= start_date) 
  if sample:
      info("Sampling rows")
      df = df.sample(0.05)
  else:
      info("Not sampling rows")
  info(f"Reading from path: {path}\n{df.count()} new records after start_date: {start_date}")
  return df

def process_raw_to_bronze(df):
  return (
    df.withColumn("loaded_at", F.current_timestamp())
    )  

def main_bronze(spark, job_name, pipeline_mode, start_date, sample):

  c = load_config(job_name="BRONZE JOB")
  info(f"Starting job {c.job_name} in {c.env} environment.")

  bronze_csv = {
    "daily_staffing_raw": f"{c.source_dir}/daily_nurse_staffing.csv",
    "providers_raw": f"{c.source_dir}/provider_info.csv",
    "claims_raw": f"{c.source_dir}/claims_quality_measures.csv"
  }

  if pipeline_mode == "full":
    start_date = "1900-01-01"

  elif pipeline_mode != "incremental":
    info("Invalid pipeline mode, exiting job.")
    return

  for table_name, csv_path in bronze_csv.items():
    rawDF = load_raw_input(spark, csv_path, start_date, sample)
    bronzeDF = process_raw_to_bronze(rawDF)

    overwrite_partitions(spark, df=bronzeDF, table=f"{c.bronze_db}.{table_name}", partition_col="ingest_date")

  table_counts(spark, schemas=[c.bronze_fqn])
  info(f"Completed job: {c.job_name}.")
