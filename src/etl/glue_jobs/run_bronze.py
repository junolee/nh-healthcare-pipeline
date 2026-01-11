import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

from config import *
from main_bronze import main_bronze


params = ['JOB_NAME', 'PIPELINE_MODE', 'START_DATE', 'SAMPLE']
args = getResolvedOptions(sys.argv, params)
job_name = args.get("JOB_NAME")
pipeline_mode = args.get("PIPELINE_MODE")
start_date = args.get("START_DATE")
sample = True if args["SAMPLE"].lower() == "true" else False


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")


info(f"JOB_NAME: {job_name}, PIPELINE_MODE: {pipeline_mode}, START_DATE: {start_date}, SAMPLE: {sample}")
main_bronze(spark=spark, job_name=job_name, pipeline_mode=pipeline_mode, start_date=start_date, sample=sample)

