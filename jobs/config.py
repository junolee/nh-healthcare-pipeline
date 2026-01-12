from dataclasses import dataclass
import pyspark.sql.functions as F
from pyspark.sql import Window

# ============================================================================
# CONFIG CLASS 
# ============================================================================

@dataclass
class PipelineConfig:
  env: str
  source_dir: str
  warehouse_dir: str
  catalog: str
  job_name: str
  mode: str
  bronze_db: str = "nh_bronze"
  silver_db: str = "nh_silver"
  gold_db: str = "nh_gold"

  @property
  def bronze_fqn(self) -> str:
    if self.catalog == "spark_catalog":
      return self.bronze_db
    return f"{self.catalog}.{self.bronze_db}"

  @property
  def silver_fqn(self) -> str:
    if self.catalog == "spark_catalog":
      return self.silver_db
    return f"{self.catalog}.{self.silver_db}"

  @property
  def gold_fqn(self) -> str:
    if self.catalog == "spark_catalog":
      return self.gold_db
    return f"{self.catalog}.{self.gold_db}"

  @property
  def bronze_dir(self) -> str:
    return f"{self.warehouse_dir}/{self.bronze_db}.db"

  @property
  def silver_dir(self) -> str:
    return f"{self.warehouse_dir}/{self.silver_db}.db"

  @property
  def gold_dir(self) -> str:
    return f"{self.warehouse_dir}/{self.gold_db}.db"



def load_config(job_name):
  ENV = "local_test"
  MODE = "full"
  CATALOG = "spark_catalog"
  AWS_WAREHOUSE = "s3a://jl-nh-healthcare/tables"
  AWS_SOURCE_DIR = "s3a://jl-nh-healthcare/raw"

  config = PipelineConfig(
    env=ENV,
    source_dir=AWS_SOURCE_DIR,
    warehouse_dir=AWS_WAREHOUSE,
    catalog=CATALOG,
    job_name=job_name,
    mode=MODE
  )
  return config


# ============================================================================
# LOGGING UTILITIES 
# ============================================================================

def info(msg):
   print("=" * 80)
   print(msg)
   print("=" * 80)


def table_counts(spark, schemas):
  table_count_msgs = []
  for db in schemas:
    tables = spark.sql(f"SHOW TABLES IN {db}")
    table_names = [f"{r.namespace}.{r.tableName}" for r in tables.collect()]

    table_count_msgs = []
    for t in table_names: 
      table_count_msgs.append(f"{spark.table(t).count():,} records  {t}")

  print("=" * 80)
  print(f"\nTABLE ROW COUNTS:")
  print("\n".join(table_count_msgs))
  print("=" * 80)


# ============================================================================
# IO UTILITIES 
# ============================================================================

# bronze

def load_csv(spark, csv_path):
  df = spark.read.csv(path = csv_path, header=True)
  return df

def append_table_path(df, path, partition_by=None):
  if not partition_by:
    df.write.mode("append").parquet(path)
  else:
    df.write.mode("append").partitionBy(partition_by).parquet(path)

# silver

def path_exists(spark, path: str) -> bool:
    sc = spark.sparkContext
    jvm = sc._jvm
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(sc._jsc.hadoopConfiguration())  # Use the path's own URI and the Hadoop configuration to get the right FS
    return fs.exists(jpath)

def overwrite_table_path(df, path, partition_by=None):
  if not partition_by:
    df.write.mode("overwrite").parquet(path)
  else:
    df.write.mode("overwrite").partitionBy(partition_by).parquet(path)

def overwrite_table(df, table):
  df.write.mode("overwrite").insertInto(table, overwrite=True)

def overwrite_partitions(spark, df, table, partition_col):
  value_columns = [c for c in df.columns if c != partition_col]
  df = df.select(*value_columns, partition_col)
  df.write.mode("overwrite").insertInto(table, overwrite=True)
  spark.sql(f"MSCK REPAIR TABLE {table}")


# ============================================================================
# TRANSFORM UTILITIES 
# ============================================================================-

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
    
  RENAME_PAIRS = [
    ("CMS Certification Number (CCN)", "provider_id"),
    ("Provider Name", "provider_name"),
    ("Provider Address", "provider_address"),
    ("City/Town", "provider_city"),
    ("State", "provider_state"),
    ("ZIP Code", "provider_zip_code"),
    ("Measure Code", "measure_code"),
    ("Measure Description", "measure_description"),
    ("Resident type", "resident_type"),
    ("Adjusted Score", "adjusted_score"),
    ("Observed Score", "observed_score"),
    ("Expected Score", "expected_score"),
    ("Footnote for Score", "measure_score_footnote"),
    ("Used in Quality Measure Five Star Rating", "used_in_five_star_rating"),
    ("Measure Period", "measure_period"),
    ("Location", "provider_full_address"),
    ("Processing Date", "processing_date"),
    ("loaded_at", "loaded_at"),
    ("ingest_date", "ingest_date")
  ]

  TARGET_TYPES = {
    "provider_id": "string",
    "provider_zip_code": "string",
    "measure_code": "int",
    "measure_score_footnote": "int",
    "adjusted_score": "double",
    "observed_score": "double",
    "expected_score": "double",
    "used_in_five_star_rating": "string",
    "resident_type": "string",
    "measure_period": "string",  # "yyyymmdd - yyyymmdd" stays string until parsed
    "processing_date": "date",
    "loaded_at": "timestamp",
    "ingest_date": "date",
  }

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

  RENAME_PAIRS = [
    ("CMS Certification Number (CCN)", "provider_id"),
    ("Provider Name", "provider_name"),
    ("Provider Address", "provider_address"),
    ("City/Town", "provider_city"),
    ("State", "provider_state"),
    ("ZIP Code", "provider_zip_code"),
    ("Telephone Number", "provider_phone_number"),
    ("Provider SSA County Code", "provider_ssa_county_code"),
    ("County/Parish", "provider_county_name"),
    ("Ownership Type", "ownership_type"),
    ("Number of Certified Beds", "num_certified_beds"),
    ("Average Number of Residents per Day", "avg_residents_per_day"),
    ("Average Number of Residents per Day Footnote", "avg_residents_per_day_footnote"),
    ("Provider Type", "provider_type"),
    ("Provider Resides in Hospital", "provider_in_hospital_flag"),
    ("Legal Business Name", "legal_business_name"),
    ("Date First Approved to Provide Medicare and Medicaid Services", "first_approved_date"),
    ("Affiliated Entity Name", "affiliated_entity_name"),
    ("Affiliated Entity ID", "affiliated_entity_id"),
    ("Continuing Care Retirement Community", "ccrc_flag"),
    ("Special Focus Status", "special_focus_status"),
    ("Abuse Icon", "abuse_icon_flag"),
    ("Most Recent Health Inspection More Than 2 Years Ago", "health_inspection_gt2yrs_flag"),
    ("Provider Changed Ownership in Last 12 Months", "ownership_change_12mo_flag"),
    ("With a Resident and Family Council", "resident_family_council"),
    ("Automatic Sprinkler Systems in All Required Areas", "sprinkler_all_required_areas"),
    ("Overall Rating", "overall_rating"),
    ("Overall Rating Footnote", "overall_rating_footnote"),
    ("Health Inspection Rating", "health_inspection_rating"),
    ("Health Inspection Rating Footnote", "health_inspection_rating_footnote"),
    ("QM Rating", "qm_rating"),
    ("QM Rating Footnote", "qm_rating_footnote"),
    ("Long-Stay QM Rating", "long_stay_qm_rating"),
    ("Long-Stay QM Rating Footnote", "long_stay_qm_rating_footnote"),
    ("Short-Stay QM Rating", "short_stay_qm_rating"),
    ("Short-Stay QM Rating Footnote", "short_stay_qm_rating_footnote"),
    ("Staffing Rating", "staffing_rating"),
    ("Staffing Rating Footnote", "staffing_rating_footnote"),
    ("Reported Staffing Footnote", "reported_staffing_footnote"),
    ("Physical Therapist Staffing Footnote", "pt_staffing_footnote"),
    ("Reported Nurse Aide Staffing Hours per Resident per Day", "reported_aide_hrs_resday"),
    ("Reported LPN Staffing Hours per Resident per Day", "reported_lpn_hrs_resday"),
    ("Reported RN Staffing Hours per Resident per Day", "reported_rn_hrs_resday"),
    ("Reported Licensed Staffing Hours per Resident per Day", "reported_licensed_hrs_resday"),
    ("Reported Total Nurse Staffing Hours per Resident per Day", "reported_total_nurse_hrs_resday"),
    ("Total number of nurse staff hours per resident per day on the weekend", "reported_weekend_total_nurse_hrs_resday"),
    ("Registered Nurse hours per resident per day on the weekend", "reported_weekend_rn_hrs_resday"),
    ("Reported Physical Therapist Staffing Hours per Resident Per Day", "reported_pt_hrs_resday"),
    ("Total nursing staff turnover", "total_nurse_turnover"),
    ("Total nursing staff turnover footnote", "total_nurse_turnover_footnote"),
    ("Registered Nurse turnover", "rn_turnover"),
    ("Registered Nurse turnover footnote", "rn_turnover_footnote"),
    ("Number of administrators who have left the nursing home", "administrator_turnover_count"),
    ("Administrator turnover footnote", "administrator_turnover_footnote"),
    ("Nursing Case-Mix Index", "nursing_case_mix_index"),
    ("Nursing Case-Mix Index Ratio", "nursing_case_mix_index_ratio"),
    ("Case-Mix Nurse Aide Staffing Hours per Resident per Day", "case_mix_aide_hrs_resday"),
    ("Case-Mix LPN Staffing Hours per Resident per Day", "case_mix_lpn_hrs_resday"),
    ("Case-Mix RN Staffing Hours per Resident per Day", "case_mix_rn_hrs_resday"),
    ("Case-Mix Total Nurse Staffing Hours per Resident per Day", "case_mix_total_nurse_hrs_resday"),
    ("Case-Mix Weekend Total Nurse Staffing Hours per Resident per Day", "case_mix_weekend_total_nurse_hrs_resday"),
    ("Adjusted Nurse Aide Staffing Hours per Resident per Day", "adjusted_aide_hrs_resday"),
    ("Adjusted LPN Staffing Hours per Resident per Day", "adjusted_lpn_hrs_resday"),
    ("Adjusted RN Staffing Hours per Resident per Day", "adjusted_rn_hrs_resday"),
    ("Adjusted Total Nurse Staffing Hours per Resident per Day", "adjusted_total_nurse_hrs_resday"),
    ("Adjusted Weekend Total Nurse Staffing Hours per Resident per Day", "adjusted_weekend_total_nurse_hrs_resday"),
    ("Rating Cycle 1 Standard Survey Health Date", "rating_cycle1_standard_survey_date"),
    ("Rating Cycle 1 Total Number of Health Deficiencies", "rating_cycle1_total_health_deficiencies"),
    ("Rating Cycle 1 Number of Standard Health Deficiencies", "rating_cycle1_standard_health_deficiencies"),
    ("Rating Cycle 1 Number of Complaint Health Deficiencies", "rating_cycle1_complaint_health_deficiencies"),
    ("Rating Cycle 1 Health Deficiency Score", "rating_cycle1_health_deficiency_score"),
    ("Rating Cycle 1 Number of Health Revisits", "rating_cycle1_health_revisits"),
    ("Rating Cycle 1 Health Revisit Score", "rating_cycle1_health_revisit_score"),
    ("Rating Cycle 1 Total Health Score", "rating_cycle1_total_health_score"),
    ("Rating Cycle 2 Standard Health Survey Date", "rating_cycle2_standard_survey_date"),
    ("Rating Cycle 2 Total Number of Health Deficiencies", "rating_cycle2_total_health_deficiencies"),
    ("Rating Cycle 2 Number of Standard Health Deficiencies", "rating_cycle2_standard_health_deficiencies"),
    ("Rating Cycle 2 Number of Complaint Health Deficiencies", "rating_cycle2_complaint_health_deficiencies"),
    ("Rating Cycle 2 Health Deficiency Score", "rating_cycle2_health_deficiency_score"),
    ("Rating Cycle 2 Number of Health Revisits", "rating_cycle2_health_revisits"),
    ("Rating Cycle 2 Health Revisit Score", "rating_cycle2_health_revisit_score"),
    ("Rating Cycle 2 Total Health Score", "rating_cycle2_total_health_score"),
    ("Rating Cycle 3 Standard Health Survey Date", "rating_cycle3_standard_survey_date"),
    ("Rating Cycle 3 Total Number of Health Deficiencies", "rating_cycle3_total_health_deficiencies"),
    ("Rating Cycle 3 Number of Standard Health Deficiencies", "rating_cycle3_standard_health_deficiencies"),
    ("Rating Cycle 3 Number of Complaint Health Deficiencies", "rating_cycle3_complaint_health_deficiencies"),
    ("Rating Cycle 3 Health Deficiency Score", "rating_cycle3_health_deficiency_score"),
    ("Rating Cycle 3 Number of Health Revisits", "rating_cycle3_health_revisits"),
    ("Rating Cycle 3 Health Revisit Score", "rating_cycle3_health_revisit_score"),
    ("Rating Cycle 3 Total Health Score", "rating_cycle3_total_health_score"),
    ("Total Weighted Health Survey Score", "total_weighted_health_survey_score"),
    ("Number of Facility Reported Incidents", "facility_reported_incidents"),
    ("Number of Substantiated Complaints", "substantiated_complaints"),
    ("Number of Citations from Infection Control Inspections", "infection_control_citations"),
    ("Number of Fines", "fines_count"),
    ("Total Amount of Fines in Dollars", "fines_total_dollars"),
    ("Number of Payment Denials", "payment_denials_count"),
    ("Total Number of Penalties", "total_penalties_count"),
    ("Location", "provider_full_address"),
    ("Latitude", "provider_latitude"),
    ("Longitude", "provider_longitude"),
    ("Geocoding Footnote", "geocoding_footnote"),
    ("Processing Date", "processing_date"),
    ("loaded_at", "loaded_at"),
    ("ingest_date", "ingest_date"),
  ]

  TARGET_TYPES = {
    # keep identifiers as string to preserve leading zeros
    "provider_id": "string",
    "provider_zip_code": "string",
    "provider_ssa_county_code": "string",
    "provider_phone_number": "string",
    
    "num_certified_beds": "int",
    "facility_reported_incidents": "int",
    "substantiated_complaints": "int",
    "infection_control_citations": "int",
    "fines_count": "int",
    "payment_denials_count": "int",
    "total_penalties_count": "int",
    "administrator_turnover_count": "int",
    
    "avg_residents_per_day": "double",
    "fines_total_dollars": "double",
    "total_weighted_health_survey_score": "double",
    "nursing_case_mix_index": "double",
    "nursing_case_mix_index_ratio": "double",
    "provider_latitude": "double",
    "provider_longitude": "double",
    "first_approved_date": "date",  # Dates (handled in function logic, but listed here for reference if needed)
    "processing_date": "date",
    "loaded_at": "timestamp",
    "ingest_date": "date",
  }


  # Bulk assign types based on naming conventions
  for _, column_name in RENAME_PAIRS:
      if "hrs_resday" in column_name:
          TARGET_TYPES[column_name] = "double"
      
      elif "turnover" in column_name and "count" not in column_name and "footnote" not in column_name:
          TARGET_TYPES[column_name] = "double"
          
      # Ratings (1-5) and Deficiencies counts are integers
      elif column_name.endswith("_rating") or "deficiencies" in column_name or "revisits" in column_name:
          TARGET_TYPES[column_name] = "int"
          
      # Scores are usually integers (except the weighted total handled above)
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

  return cleanDF

def clean_staffing_levels(inputDF):
    
  RENAME_PAIRS = [
    ("PROVNUM", "provider_id"),
    ("PROVNAME", "provider_name"),
    ("CITY", "provider_city"),
    ("STATE", "provider_state"),
    ("COUNTY_NAME", "provider_county_name"),
    ("COUNTY_FIPS", "provider_county_fips"),
    ("CY_Qtr", "calendar_year_quarter"),
    ("WorkDate", "work_date"),
    ("MDScensus", "num_patients"),
    ("Hrs_RN", "hrs_registered_nurses"),
    ("Hrs_RN_emp", "hrs_registered_nurses_employee"),
    ("Hrs_RN_ctr", "hrs_registered_nurses_contracted"),
    ("Hrs_RNDON", "hrs_registered_nurses_onduty"),
    ("Hrs_RNDON_emp", "hrs_registered_nurses_onduty_employed"),
    ("Hrs_RNDON_ctr", "hrs_registered_nurses_onduty_contracted"),
    ("Hrs_RNadmin", "hrs_registered_nurse_admins"),
    ("Hrs_RNadmin_emp", "hrs_registered_nurse_admins_employee"),
    ("Hrs_RNadmin_ctr", "hrs_registered_nurse_admins_contracted"),
    ("Hrs_LPNadmin", "hrs_licensed_practical_nurse_admins"),
    ("Hrs_LPNadmin_emp", "hrs_licensed_practical_nurse_admins_employee"),
    ("Hrs_LPNadmin_ctr", "hrs_licensed_practical_nurse_admins_contracted"),
    ("Hrs_LPN", "hrs_licensed_practical_nurses"),
    ("Hrs_LPN_emp", "hrs_licensed_practical_nurses_employee"),
    ("Hrs_LPN_ctr", "hrs_licensed_practical_nurses_contracted"),
    ("Hrs_CNA", "hrs_certified_nursing_assistants"),
    ("Hrs_CNA_emp", "hrs_certified_nursing_assistants_employee"),
    ("Hrs_CNA_ctr", "hrs_certified_nursing_assistants_contracted"),
    ("Hrs_NAtrn", "hrs_nursing_assistant_trainees"),
    ("Hrs_NAtrn_emp", "hrs_nursing_assistant_trainees_employee"),
    ("Hrs_NAtrn_ctr", "hrs_nursing_assistant_trainees_contracted"),
    ("Hrs_MedAide", "hrs_medication_aides"),
    ("Hrs_MedAide_emp", "hrs_medication_aides_employee"),
    ("Hrs_MedAide_ctr", "hrs_medication_aides_contracted"),
    ("loaded_at", "loaded_at"),
    ("ingest_date", "ingest_date"),
  ]

  TARGET_TYPES = {
    "provider_id": "string",
    "provider_name": "string",
    "provider_city": "string",
    "provider_state": "string",
    "provider_county_name": "string",
    "provider_county_fips": "string",
    "calendar_year_quarter": "string",
    "work_date": "date",
    "num_patients": "int",
    "loaded_at": "timestamp",
    "ingest_date": "date",
  }
  
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

