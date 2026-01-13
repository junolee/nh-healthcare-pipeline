from config import *


def load_bronze_table(spark, table, start_date):
  df = spark.table(table)
  df = df.filter(F.col("ingest_date") >= start_date) 
  info(f"Reading from table: {table}\n{df.count()} new records after start_date: {start_date}")
  return df

def dedupe(inputDF, partitionBy):
  return (
    inputDF
      .withColumn("row_num", F.row_number().over(
        Window.partitionBy(partitionBy).orderBy(F.col("loaded_at").desc())
    ))
    .filter("row_num = 1")
    .drop("row_num")
  )

def clean_claims(inputDF):
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

  return cleanDF


def clean_providers(inputDF):

  RENAME_PAIRS = PROVIDERS_RENAME_PAIRS
  TARGET_TYPES = dict(PROVIDERS_BASE_TARGET_TYPES)

  # Bulk assign types based on naming conventions
  for _, column_name in RENAME_PAIRS:
      if "hrs_resday" in column_name:
          TARGET_TYPES[column_name] = "double"
      elif "turnover" in column_name and "count" not in column_name and "footnote" not in column_name:
          TARGET_TYPES[column_name] = "double"
      elif column_name.endswith("_rating") or "deficiencies" in column_name or "revisits" in column_name:
          TARGET_TYPES[column_name] = "int"  # Ratings (1-5) and Deficiencies counts are integers
      elif column_name.endswith("_score") and column_name != "total_weighted_health_survey_score":
          TARGET_TYPES[column_name] = "int"  # Scores are integers (except weighted total handled above)    
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

  return cleanDF

def clean_staffing_levels(inputDF):

  RENAME_PAIRS = STAFFING_LEVELS_RENAME_PAIRS
  TARGET_TYPES = dict(STAFFING_LEVELS_BASE_TARGET_TYPES)
  
  # hr_* columns default to double
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

  return cleanDF
    
def generate_dim_dates(spark, start_date, end_date):

  days_diff = spark.sql(f"SELECT datediff('{end_date}', '{start_date}')").collect()[0][0]   # compute # days first

  datesDF = spark.range(0, days_diff + 1).select(  # generate distributed IDs; scalable for billions of rows
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
  
  return dim_datesDF


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