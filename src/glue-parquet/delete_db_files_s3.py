import boto3
from dataclasses import dataclass

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
  AWS_WAREHOUSE = "s3://jl-nh-healthcare/tables"
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

c = load_config("RESET JOB")

def delete_files(bucket_name, dir_prefix):
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket_name)
  bucket.objects.filter(Prefix=dir_prefix).delete()
  print(f"Deleted all files in s3://{bucket_name}/{dir_prefix}")

if __name__ == "__main__":
  BUCKET_NAME = "jl-nh-healthcare"
  BRONZE_PREFIX = "tables/nh_bronze.db/"
  SILVER_PREFIX = "tables/nh_silver.db/"
  GOLD_PREFIX = "tables/nh_gold.db/"

  delete_files(BUCKET_NAME, BRONZE_PREFIX)
  delete_files(BUCKET_NAME, SILVER_PREFIX)
  delete_files(BUCKET_NAME, GOLD_PREFIX)