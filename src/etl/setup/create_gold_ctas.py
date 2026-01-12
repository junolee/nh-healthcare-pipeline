import queries as q

def create_gold_view(table_name, select_query):
  ddl = f"""CREATE OR REPLACE VIEW {table_name} AS {select_query}"""
  print(ddl)
  return ddl


def create_gold_table(table_name, select_query, partition_by=""):
  partition_option = f", partitioned_by = ARRAY['{partition_by}']" if partition_by else ""

  ddl = f"""
CREATE TABLE nh_gold.{table_name}
WITH (
  format = 'PARQUET',
  external_location = 's3://jl-nh-healthcare/tables/nh_gold.db/{table_name}/',
  write_compression = 'SNAPPY'{partition_option}
) AS {select_query}
  """
  print(ddl)
  return ddl



# create_gold_table("daily_provider_metrics", q.daily_provider_metrics, partition_by='date')

GOLD_QUERIES = {
  "daily_provider_metrics": q.daily_provider_metrics,
  "agg_daily_metrics": q.agg_daily_metrics,
  "agg_provider_metrics": q.agg_provider_metrics,
  "agg_state_metrics": q.agg_state_metrics
}

for table, query in GOLD_QUERIES.items():
  create_gold_table(table, query)
