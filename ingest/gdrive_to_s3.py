"""
Incremental ingestion from Google Drive to an S3 "raw" landing zone

Called by: lambda_function.py

Run modes - set via main():
- Full refresh: ingest all CSV files in the source Google Drive folder
- Incremental: ingest only CSV files that changed since the last run, based on the Google Drive Changes API checkpoint (startPageToken)

State:
- Each run produces a new startPageToken for the next incremental run
- If persist_state=True, the new token is saved to S3 at start_token_path
"""

import datetime
from pathlib import Path
from config import *


def get_relevant_changed_file(change_record, folder_id):
  """
  Return file metadata if it is relevant; otherwise return None.

  Change record is relevant only if:
  - change type is file (not drive/permission/etc)
  - file was not removed
  - file is a CSV
  - file is located in the specified folder
  """
  if change_record["removed"] or change_record["changeType"] != "file": 
    return None
  
  file = change_record["file"]
  
  if folder_id not in file["parents"]:
    return None

  if file['mimeType'] != "text/csv": 
    return None

  return file


def fetch_changed_files(drive, drive_id, folder_id, start_token):  # get all change events (paginate)
  """
  Fetch changed file IDs since start_token, and return (file_ids, new_start_token)

  - Paginates through the Drive Changes stream using list_changes()
  - Filters account-wide changes down to CSV files in the specified source folder
  """
  
  all_changes, next_page, new_start_token = list_changes(drive, drive_id, start_token, 2)

  while next_page:
    changes, next_page, new_start_token = list_changes(drive, drive_id, next_page, 2)
    all_changes += changes

  file_ids = []
  for change in all_changes:
    file = get_relevant_changed_file(change, folder_id)
    if file:
      file_ids.append(file.get('id'))
      print(f"Detected changed file: {file.get('name')}")
  
  return file_ids, new_start_token


def fetch_all_files(drive, drive_id, folder_id):
  """
  Fetch all file IDs in the source folder, and return (file_ids, new_start_token)

  - Paginates through files in specified Drive folder using list_files()
  - Creates a new startPageToken for future incremental runs
  """  
  all_files, next_page = list_files(drive, folder_id, 2)
  
  while next_page:
    files, next_page = list_files(drive, folder_id, 2, next_page)
    all_files += files  
  
  new_start_token = create_start_page_token(drive, drive_id)
  
  file_ids = [file["id"] for file in all_files if file['mimeType'] == "text/csv"]
  print(f"Fetched {len(file_ids)} files from Google Drive folder ID: {folder_id}.")

  return file_ids, new_start_token


def ingest_files(drive, s3, file_ids, bucket_name):
  """
  Download each Google Drive file in file_ids and upload it to S3.

  - Uses current date to build S3 target path with ingest_date partition.
  """
  ingest_date = datetime.datetime.now().strftime("%Y-%m-%d")  # use ingest_date to build filepath

  print(f"Loading files to S3: {bucket_name}/raw/<dataset>/ingest_date={ingest_date}/<dataset>.csv")

  for file_id in file_ids:
    filename = get_file_metadata(drive, file_id)["name"]
    dataset_name = Path(filename).stem
    path = f"raw/{dataset_name}.csv/ingest_date={ingest_date}/{dataset_name}.csv"
    
    print(f"Starting file transfer for: {filename}")
    buffer = get_file_content_buffer(drive, file_id)
    s3.upload_fileobj(buffer, Bucket=bucket_name, Key=path)
    print(f"Uploaded {filename} to S3.\n")

  print("Ingested all files.")


def main(full_refresh=False, persist_state=False):
  """
    Run ingestion in either Full Refresh or Incremental mode.

    Args:
      full_refresh: If True, ingest all CSVs in the configured Google Drive folder. If False, ingest only changed CSVs.
      persist_state: If True, save the new startPageToken (checkpoint used in Google Drive Changes API) to S3.

    Requires config (loaded via load_config()):
    - drive_id, folder_id: Google Drive + folder containing the source CSV files
    - bucket_name: S3 bucket for raw landing zone + state (see start_token_path below)
    - start_token_path: S3 path to JSON file storing startPageToken
  """
  print(f"main received args for persist_state: {persist_state}")
  
  mode = "Full Refresh" if full_refresh else "Incremental"
  print(f"Running ingest script in mode: {mode}")

  c = load_config()
  s3 = get_s3_client()
  drive = get_drive_client()

  if full_refresh:
    print(f"Fetching all files in full refresh mode")
    file_ids, new_start_token = fetch_all_files(drive, c.drive_id, c.folder_id)
  
  else:
    start_token = load_start_token(s3, c.bucket_name, c.start_token_path)
    print(f"Fetching all files in incremental mode based on start token: {start_token}")
    file_ids, new_start_token = fetch_changed_files(drive, c.drive_id, c.folder_id, start_token)

    print(f"Found the following changed files and received new start token {new_start_token}")
    print(file_ids)
  
  ingest_files(drive, s3, file_ids, c.bucket_name)

  if persist_state:
    print("Persisting state...")
    save_start_page_token(s3, c.bucket_name, c.start_token_path, new_start_token)
    print("Updated start page token for next incremental load")
  else:
    print("Not persisting state...")
  
  return
  