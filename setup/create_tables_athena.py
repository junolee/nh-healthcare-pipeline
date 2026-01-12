import schemas as s


WAREHOUSE_DIR="s3://jl-nh-healthcare/tables"
BRONZE_DB = "nh_bronze"
SILVER_DB = "nh_silver"
GOLD_DB = "nh_gold"

def create_external_parquet_table(db_name, table_name, columns_sql, partition_by=""):

  partition_clause = f"PARTITIONED BY ({partition_by} date)" if partition_by else ""


  ddl = f"""
CREATE EXTERNAL TABLE {db_name}.{table_name} ({columns_sql}
) 
{partition_clause}
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '{WAREHOUSE_DIR}/{db_name}.db/{table_name}/'
  """
  
  print(ddl)
  return ddl


BRONZE_COLUMNS = {
  "daily_staffing_raw": s.DAILY_STAFFING_RAW_COLUMNS,
  "providers_raw": s.PROVIDERS_RAW_COLUMNS,
  "claims_raw": s.CLAIMS_RAW_COLUMNS
}
SILVER_COLUMNS = {
  "fct_staffing_levels": s.FCT_STAFFING_LEVELS_COLUMNS,
  "dim_providers": s.DIM_PROVIDERS_COLUMNS,
  "dim_claims": s.DIM_CLAIMS_COLUMNS,
  "dim_dates": s.DIM_DATES_COLUMNS
}



for table, columns in BRONZE_COLUMNS.items():
  columns += """
loaded_at       timestamp
  """
  create_external_parquet_table(BRONZE_DB, table, columns, partition_by="ingest_date")
  
for table, columns in SILVER_COLUMNS.items():

  if table == "fct_staffing_levels":
    columns += """
  loaded_at       timestamp,
  updated_at      timestamp
    """
    create_external_parquet_table(SILVER_DB, table, columns, partition_by="ingest_date")
  elif table == "dim_dates":
    columns += """
  updated_at      timestamp
    """    
    create_external_parquet_table(SILVER_DB, table, columns)
  else:
    columns += """
  loaded_at       timestamp,
  updated_at      timestamp,
  ingest_date     date
    """
    create_external_parquet_table(SILVER_DB, table, columns)

  
