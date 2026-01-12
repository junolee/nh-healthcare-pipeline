# NH Healthcare Metrics Pipeline

## Business goal

Build clean, analytics-ready datasets to study daily nurse staffing levels and how they relate to resident levels and quality scores across facilities.

Inputs:

* Facility details (attributes, resident load, etc.)  
* Daily staffing levels by facility (registered nurses, LPN, employed vs contract, etc.)  
* Medicare claims (quality measures, type of stay, etc.)

Outputs:

* `fct_staffing_levels` \- grain: (provider\_id, work\_date)  
* `dim_date` \- grain: date\_id  
* `dim_providers` \- grain: (provider\_id, measure\_code)  
* `dim_claims` \- grain: provider\_id  
* `facility_measures` \- denormalized view used by Python

## Architecture

![](nh-arch.png)

#### **Storage layout**

* Google Drive contains CSV data for each dataset  
* Top-level directories in S3 bucket:  
  * `raw:` landing area for raw CSV files; folder per dataset  
  * `bronze:` stores parquet files registered as external tables in Glue bronze database  
  * `silver:` parquet files registered as external tables in Glue silver database  
  * `gold:` parquet files registered as external tables in Glue gold database  
  * `state:` used by ingest lambda to track incremental extractions from google drive (startPageToken)  
* Glue Data Catalog stores the following schemas and objects  
  * `bronze:` raw tables tagged with metadata  
  * `silver:` fact & SCD2 dimension tables  
  * `gold:` analytics view for dashboard

#### **Ingestion**

* Lambda function `gdrive_to_s3` ingest raw CSVs from Google Drive  
  * Triggered via event notification for new files (or on scheduled runs)  
  * Authenticates to google drive via oath token stored in AWS Secrets Manager  
  * Incrementally extract files via Google Drive Changes API  
    * Stores startPageToken (checkpoint) in s3 subpath: `state/google-drive/startPageToken.json`  
  * Loads extracted CSV files into S3 raw landing zone

#### **Pipeline (Glue ETL Jobs)**

* Job: Append raw to bronze tables w/metadata  
  * `daily_nurse_staffing_raw`  
  * `providers_raw`  
  * `medicare_claims_raw`   
* Job: Incrementally process and merge updates into silver tables  
  * `fct_staffing_levels`  
  * `dim_providers`  
  * `dim_claims`  
  * `dim_date`  
* Job: Build denormalized analytics view  
  * `facility_measures`

#### **Consumption**

* Query tables via Athena through Glue Data Catalog  
* Connect Streamlit dashboard to Athena

## Dashboard
Dashboard: https://nh-healthcare.streamlit.app

![](nh-dashboard-top.png)

![](nh-dashboard-bottom.png)

