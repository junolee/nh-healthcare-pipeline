"""
Bronze ETL Job - Raw CSV to Bronze Parquet Tables in Glue Catalog

Called by: run_bronze.py

Run modes - set via main_bronze():
- full: loads all data in the raw CSV directory
- incremental: loads partitions with ingest_date >= start_date

Inputs: Raw CSVs in S3 with ingest_date column
Outputs: Glue tables in bronze database (partitioned by ingest_date)
"""

from config import *
import pyspark.sql.functions as F

def load_csv(spark, csv_path):
  df = spark.read.csv(path = csv_path, header=True)
  return df

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

def overwrite_partitions(spark, df, table, partition_col):
  """
  Overwrite `table` only for partitions present in `df` (dynamic partition overwrite)
  Run MSCK REPAIR TABLE to refresh partition metadata in Glue Catalog
  """
  value_columns = [c for c in df.columns if c != partition_col]
  df = df.select(*value_columns, partition_col) # ensure partition col is last
  df.write.mode("overwrite").insertInto(table, overwrite=True)
  spark.sql(f"MSCK REPAIR TABLE {table}")

def main_bronze(spark, job_name, pipeline_mode, start_date, sample):
  """
  Build bronze tables from raw CSVs.

  Args:
    pipeline_mode: "full" loads all data
                   "incremental" loads partitions ingest_date >= start_date
    start_date: lower bound for ingest_date filter (YYYY-MM-DD) in incremental mode
    sample: (for development) if True, sample ~5% of rows

  - Reads raw CSV paths from source directory (filtered by ingest_date)
  - Adds loaded_at ingestion timestamp
  - Writes to external parquet table in bronze database, partitioned by ingest_date
  - Overwrites affected partition(s); refresh partition metadata via MSCK REPAIR TABLE
  """  

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
