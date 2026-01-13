"""
Silver ETL Job - Bronze Tables to Cleaned Silver Tables in Glue Catalog

Called by: run_silver.py

Run modes - set via main_silver():
- full: rebuilds silver tables from all bronze data
- incremental: rebuilds fact table partitions with ingest_date >= start_date (dims still rebuilt)

Inputs: Glue bronze tables (include ingest_date + loaded_at)
Outputs: Glue tables in silver database (typed, deduped, with updated_at)
"""

from config import *

def load_bronze_table(spark, table, start_date):  
  """
  Read bronze table + filter ingest_date by start_date (used for incremental processing)
  """  
  df = spark.table(table)
  df = df.filter(F.col("ingest_date") >= start_date) 
  info(f"Reading from table: {table}\n{df.count()} new records after start_date: {start_date}")
  return df

def dedupe(inputDF, key):
  """
  Deduplicate to latest per key based on loaded_at
  - key: string or list of string column names
  """  
  return (
    inputDF
      .withColumn("row_num", F.row_number().over(
        Window.partitionBy(key).orderBy(F.col("loaded_at").desc())
    ))
    .filter("row_num = 1")
    .drop("row_num")
  )

def clean_claims(inputDF):
  """
  Normalize claims data
  - rename columns using CLAIMS_RENAME_PAIRS (includes all columns)
  - cast types using CLAIMS_TARGET_TYPES where specified (others default to string)
  - parse dates: processing_date and ingest_date (yyyy-MM-dd)
  - dedupe to latest per (provider_id, measure_code), add updated_at
  """  
  RENAME_PAIRS = CLAIMS_RENAME_PAIRS
  TARGET_TYPES = CLAIMS_TARGET_TYPES

  exprs = []
  for raw_name, clean_name in RENAME_PAIRS:
      if clean_name in ["processing_date", "ingest_date"]:
          exprs.append(F.to_date(F.col(raw_name), "yyyy-MM-dd").alias(clean_name))
      else:
          exprs.append(F.col(raw_name).cast(TARGET_TYPES.get(clean_name, "string")).alias(clean_name))

  info(exprs)
  normalizedDF = inputDF.select(*exprs)
  cleanDF = dedupe(normalizedDF, ["provider_id", "measure_code"])

  return cleanDF.withColumn("updated_at", F.current_timestamp())


def clean_providers(inputDF):
  """
  Normalize providers data
  - rename columns using PROVIDERS_RENAME_PAIRS (includes all columns)
  - cast types using PROVIDERS_BASE_TARGET_TYPES where specified; otherwise infer types from column names:
    - int: endswith("_rating"), contains("footnote"), contains("deficiencies"), contains("revisits"),
           endswith("_score") except total_weighted_health_survey_score
    - double: contains("hrs_resday"), contains("turnover") except when also contains("footnote") or contains("count")
    - others default to string  
  - parse dates: processing_date and ingest_date (yyyy-MM-dd)
  - dedupe to latest per provider_id, add updated_at
  """

  RENAME_PAIRS = PROVIDERS_RENAME_PAIRS
  TARGET_TYPES = dict(PROVIDERS_BASE_TARGET_TYPES)

  # bulk assign types based on naming conventions
  for _, column_name in RENAME_PAIRS:
      if "hrs_resday" in column_name:
          TARGET_TYPES[column_name] = "double"
      elif "turnover" in column_name and "count" not in column_name and "footnote" not in column_name:
          TARGET_TYPES[column_name] = "double"
      elif column_name.endswith("_rating") or "deficiencies" in column_name or "revisits" in column_name:
          TARGET_TYPES[column_name] = "int"
      elif column_name.endswith("_score") and column_name != "total_weighted_health_survey_score":
          TARGET_TYPES[column_name] = "int"
      elif "footnote" in column_name:
          TARGET_TYPES[column_name] = "int"

  exprs = []
  for raw_name, clean_name in RENAME_PAIRS:
      if clean_name in ["processing_date", "ingest_date"]:
          exprs.append(F.to_date(F.col(raw_name), "yyyy-MM-dd").alias(clean_name))
      else:
          exprs.append(F.col(raw_name).cast(TARGET_TYPES.get(clean_name, "string")).alias(clean_name))

  normalizedDF = inputDF.select(*exprs)
  cleanDF = dedupe(normalizedDF, "provider_id")

  return cleanDF.withColumn("updated_at", F.current_timestamp())

def clean_staffing_levels(inputDF):
  """
  Normalize daily staffing data
  - rename columns using STAFFING_LEVELS_RENAME_PAIRS (includes all columns)
  - cast types using STAFFING_LEVELS_BASE_TARGET_TYPES where specified (cast `hrs_*` columns to double)
  - parse dates: work_date (yyyyMMdd) and ingest_date (yyyy-MM-dd)
  - dedupe to latest per (provider_id, work_date), add updated_at
  """  

  RENAME_PAIRS = STAFFING_LEVELS_RENAME_PAIRS
  TARGET_TYPES = dict(STAFFING_LEVELS_BASE_TARGET_TYPES)
  
  hr_columns = [t for _, t in RENAME_PAIRS if t.startswith("hrs_") and t not in TARGET_TYPES]
  for col in hr_columns:
      TARGET_TYPES[col] = "double"

  exprs = []
  for raw_name, clean_name in RENAME_PAIRS:
      if clean_name == "work_date":
          exprs.append(F.to_date(F.col(raw_name), "yyyyMMdd").alias(clean_name))
      elif clean_name == "ingest_date":
          exprs.append(F.to_date(F.col(raw_name), "yyyy-MM-dd").alias(clean_name))    
      else:
          exprs.append(F.col(raw_name).cast(TARGET_TYPES.get(clean_name, "string")).alias(clean_name))

  normalizedDF = inputDF.select(*exprs)
  cleanDF = dedupe(normalizedDF, ["provider_id", "work_date"])

  return cleanDF.withColumn("updated_at", F.current_timestamp())
    
def generate_dim_dates(spark, start_date, end_date):
  """
  Generate dim_dates for a fixed date range + add common date attributes (e.g. day_of_week, is_weekday)
  """

  days_diff = spark.sql(f"SELECT datediff('{end_date}', '{start_date}')").collect()[0][0]   # compute # days first

  datesDF = spark.range(0, days_diff + 1).select( 
      F.date_add(F.lit(start_date), F.col("id").cast("int")).alias("date")
  )
  dim_datesDF = (
    datesDF
      .withColumn("date_key",           F.date_format("date", "yyyyMMdd").cast("int"))
      .withColumn("year",               F.year("date"))
      .withColumn("cy_quarter",         F.date_format("date", "qqq"))
      .withColumn("month",              F.month("date"))
      .withColumn("month_name",         F.date_format(F.col("date"), "MMM"))
      .withColumn("day",                F.dayofmonth("date"))
      .withColumn("week_of_year",       F.weekofyear("date"))
      .withColumn("day_of_week",        F.weekday("date"))
      .withColumn("day_of_week_name",   F.date_format("date", "E"))
      .withColumn("is_weekday",        (F.col("day_of_week") <= 4).cast("boolean"))
  )
  return dim_datesDF.withColumn("updated_at", F.current_timestamp())

def overwrite_table(df, table):
  """
  Overwrite a non-partitioned Glue table (full rebuild)
  """  
  df.write.mode("overwrite").insertInto(table, overwrite=True)

def overwrite_partitions(spark, df, table, partition_col):
  """
  Overwrite `table` only for partitions present in `df` (dynamic partition overwrite)
  Run MSCK REPAIR TABLE to refresh partition metadata in Glue Catalog
  """  
  value_columns = [c for c in df.columns if c != partition_col]
  df = df.select(*value_columns, partition_col) # ensure partition col is last
  df.write.mode("overwrite").insertInto(table, overwrite=True)
  spark.sql(f"MSCK REPAIR TABLE {table}")

def main_silver(spark, job_name, pipeline_mode, start_date):
  """
  Build silver tables from bronze.

  Args:
    pipeline_mode: "full" rebuilds all silver tables
                   "incremental" rebuilds fact table partitions with ingest_date >= start_date
    start_date: lower bound for ingest_date filter (YYYY-MM-DD) in incremental mode

  Behavior:
  - dim_claims, dim_providers: rebuilt from all bronze history each run (via overwrite_table)
  - fct_staffing_levels: rebuilt only for ingest_date partitions >= start_date (via overwrite_partitions)
  - dim_dates: generated from a fixed date range and overwritten each run
  """

  c = load_config(job_name="SILVER JOB")
  info(f"Starting job {c.job_name} in {c.env} environment.")

  silver_tables = {
    "dim_claims":           ("claims_raw", clean_claims),
    "dim_providers":        ("providers_raw", clean_providers),
    "fct_staffing_levels":  ("daily_staffing_raw", clean_staffing_levels)
  }

  NO_START_DATE = "1900-01-01"
  if pipeline_mode == "full": 
    start_date = NO_START_DATE

  for silver_table, (bronze_table, transform_function) in silver_tables.items():
    

    if silver_table == "fct_staffing_levels":
      inputDF = load_bronze_table(spark, f"{c.bronze_fqn}.{bronze_table}", start_date)
      cleanDF = transform_function(inputDF)
      overwrite_partitions(spark, cleanDF, table=f"{c.silver_db}.{silver_table}", partition_col="ingest_date")
    else:
      inputDF = load_bronze_table(spark, f"{c.bronze_fqn}.{bronze_table}", NO_START_DATE)
      cleanDF = transform_function(inputDF)
      overwrite_table(cleanDF, table=f"{c.silver_db}.{silver_table}")


  dim_datesDF = generate_dim_dates(spark, start_date="2024-04-01", end_date="2024-06-30")
  overwrite_table(dim_datesDF, table=f"{c.silver_db}.dim_dates")

  table_counts(spark, schemas=[c.bronze_fqn, c.silver_fqn])
  info(f"Completed job: {c.job_name}.")