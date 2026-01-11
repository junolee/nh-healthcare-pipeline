

# Run on AWS
Configure AWS Glue jobs with job parameters:
- Key: `--PIPELINE_MODE`, Value: `incremental` (or `full`)
- Key: `--START_DATE`, Value: `YYYY-MM-DD` e.g. `2026-01-05
- Key: `--SAMPLE`, Value: `true`/`false`

## Step Functions state machine
```json
{
  "Comment": "Ingest Lambda -> Bronze Job -> Silver Job",
  "StartAt": "IngestLambda",
  "States": {
    "IngestLambda": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "gdrive_to_s3",
        "Payload.$": "$"
      },
      "Next": "BronzeJob"
    },
    "BronzeJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "bronze-job",
        "Arguments": {
          "--JOB_NAME": "bronze-job",
          "--PIPELINE_MODE": "incremental",
          "--START_DATE": "2026-01-06",
          "--SAMPLE": "false"
        }
      },
      "Next": "SilverJob"
    },
    "SilverJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "silver-job",
        "Arguments": {
          "--JOB_NAME": "silver-job",
          "--PIPELINE_MODE": "incremental",
          "--START_DATE": "2026-01-06",
          "--SAMPLE": "false"
        }
      },
      "End": true
    }
  },
  "TimeoutSeconds": 600
}
```



# Run Locally
Setup local environment
```
python -m venv venv_etl
source venv_etl/bin/activate
pip install -r requirements.txt
```

Pull docker image (AWS Docs: [Develop and test AWS Glue jobs locally using a Docker image](https://docs.aws.amazon.com/glue/latest/dg/develop-local-docker-image.html))
```bash
docker pull public.ecr.aws/glue/aws-glue-libs:5 
```

Created named profile my-glue-profile using access key 'local-glue-container' for IAM user with required permissions

Specify named profile, glue workspace directory, script name, additional python files, job params

### BRONZE JOB
```bash
export PROFILE_NAME=my-glue-profile
export WORKSPACE_LOCATION=/Users/juno/dev/dea/nh-healthcare-pipeline/src/etl/glue_jobs

export SCRIPT_FILE_NAME=run_bronze.py
export EXTRA_PY_FILES=/home/hadoop/workspace/config.py,/home/hadoop/workspace/main_bronze.py
export JOB_NAME=bronze-local-job
export PIPELINE_MODE=incremental
export START_DATE=2026-01-04
export SAMPLE=false

docker run -it --rm -v ~/.aws:/home/hadoop/.aws -v $WORKSPACE_LOCATION:/home/hadoop/workspace/ -e AWS_PROFILE=$PROFILE_NAME --name glue5_spark_submit public.ecr.aws/glue/aws-glue-libs:5 spark-submit /home/hadoop/workspace/$SCRIPT_FILE_NAME --extra-py-files $EXTRA_PY_FILES --JOB_NAME $JOB_NAME --PIPELINE_MODE $PIPELINE_MODE --START_DATE $START_DATE --SAMPLE $SAMPLE
```
### SILVER JOB
```bash
export PROFILE_NAME=my-glue-profile
export WORKSPACE_LOCATION=/Users/juno/dev/dea/nh-healthcare-pipeline/src/etl/glue_jobs

export SCRIPT_FILE_NAME=run_silver.py
export EXTRA_PY_FILES=/home/hadoop/workspace/config.py,/home/hadoop/workspace/main_silver.py
export JOB_NAME=silver-local-job
export PIPELINE_MODE=incremental
export START_DATE=2026-01-04

docker run -it --rm -v ~/.aws:/home/hadoop/.aws -v $WORKSPACE_LOCATION:/home/hadoop/workspace/ -e AWS_PROFILE=$PROFILE_NAME --name glue5_spark_submit public.ecr.aws/glue/aws-glue-libs:5 spark-submit /home/hadoop/workspace/$SCRIPT_FILE_NAME --extra-py-files $EXTRA_PY_FILES --JOB_NAME $JOB_NAME --PIPELINE_MODE $PIPELINE_MODE --START_DATE $START_DATE
```
Formatted command (above) for running container via spark-submit

```bash
docker run -it --rm \
  -v ~/.aws:/home/hadoop/.aws \
  -v $WORKSPACE_LOCATION:/home/hadoop/workspace/ \
  -e AWS_PROFILE=$PROFILE_NAME \
  --name glue5_spark_submit \
  public.ecr.aws/glue/aws-glue-libs:5 \
  spark-submit /home/hadoop/workspace/$SCRIPT_FILE_NAME \
    --extra-py-files $EXTRA_PY_FILES \
    --JOB_NAME $JOB_NAME \
    --PIPELINE_MODE $PIPELINE_MODE \
    --START_DATE $START_DATE \
    --SAMPLE $SAMPLE 
```


## To start REPL for pyspark instead
Spark session is provided via `spark`

```bash
export PROFILE_NAME=my-glue-profile

docker run -it --rm \
  -v ~/.aws:/home/hadoop/.aws \
  -e AWS_PROFILE=$PROFILE_NAME \
  --name glue5_pyspark \
  public.ecr.aws/glue/aws-glue-libs:5 \
  pyspark
```

### Reset 
Run `python ../delete_db_files_s3.py` to delete all files under bronze.db and silver.db in S3 to "truncate" tables by deleting underlying data.