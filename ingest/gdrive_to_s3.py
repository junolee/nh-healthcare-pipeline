import io
import os
import datetime
import boto3
import json
from pathlib import Path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


def _load_secret(secret_name) -> dict: # returns secret dict
  secrets = boto3.client('secretsmanager')
  secret_str = secrets.get_secret_value(SecretId=secret_name)['SecretString']
  return json.loads(secret_str)
  

def _save_secret_str(secret_name, data: str): # data will be stored as JSON string
  secrets = boto3.client('secretsmanager')
  secrets.put_secret_value(SecretId=secret_name, SecretString=data)


def get_drive_client():
  SECRET_NAME = os.environ["OAUTH_SECRET_NAME"]  
  token = _load_secret(SECRET_NAME)

  creds = Credentials.from_authorized_user_info(token)

  if creds.expired and creds.refresh_token: 
    creds.refresh(Request())
    print("Refreshed token")

    _save_secret_str(SECRET_NAME, creds.to_json())
    print("Updated secret with refreshed token")

  else:
    print("Using token loaded from secret")
    
  drive = build('drive', 'v3', credentials=creds)
  print("Created drive service")
  return drive

def _list_files(drive, parent_folder, page_size=None, page_token=None, fields=None):
  results = drive.files().list(
      q=f"'{parent_folder}' in parents",   
      fields=fields if fields else "nextPageToken,files(id,name,mimeType,parents)",
      pageSize=page_size,
      includeItemsFromAllDrives=True,
      supportsAllDrives=True,
      pageToken=page_token
    ).execute()
  
  files = results.get('files')
  next_page = results.get("nextPageToken")

  return files, next_page

def _create_start_page_token(drive, DRIVE_ID):
  results = drive.changes().getStartPageToken(
    driveId=DRIVE_ID,
    supportsAllDrives=True
  ).execute()
  return results.get("startPageToken")

def _list_changes(drive, DRIVE_ID, page_token, page_size=None, fields=None):
  results = drive.changes().list(
    pageToken=page_token,
    pageSize=page_size,
    driveId=DRIVE_ID,
    supportsAllDrives=True,
    includeItemsFromAllDrives=True,
    fields=fields if fields else "changes(changeType,time,removed,fileId,file(id,name,mimeType,parents)),newStartPageToken,nextPageToken"
  ).execute()

  changes = results.get('changes')
  next_page = results.get("nextPageToken")
  new_start_page = results.get("newStartPageToken")
  
  return changes, next_page, new_start_page

def _get_file_metadata(drive, file_id, fields=None):
  results = drive.files().get(
    fileId=file_id,
    supportsAllDrives=True,
    fields=fields if fields else "id,name,mimeType,modifiedTime,trashed,md5Checksum,size"
  ).execute()
  return results


def _get_file_content_buffer(drive, file_id):
  buffer = io.BytesIO()
  request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
  downloader = MediaIoBaseDownload(fd=buffer, request=request)
  done = False
  while not done: 
    status, done = downloader.next_chunk()
    if status: print(f"Download {int(status.progress() * 100)}%")
  buffer.seek(0)
  return buffer


def _relevant_updated_file(change, FOLDER_ID):
  """Returns the changed file object if relevant, returns None if not."""
  if change["removed"] or change["changeType"] != "file": return False
  file = change["file"]
  if FOLDER_ID not in file["parents"] or file['mimeType'] != "text/csv": return False
  return file

def _fetch_changed_files(drive, DRIVE_ID, FOLDER_ID, start_token):  # get all change events (paginate)
  """
  Returns list of relevant updated file_ids + new start token
  - makes API calls to drive.changes().list(), paginates for complete results
  - filters account-wide drive change stream for relevant updates to files in source directory
  - returns new start page token to track incremental updates with changes API
  """
  
  all_changes, next_page, new_start_token = _list_changes(drive, DRIVE_ID, start_token, 2)
  while next_page:
    changes, next_page, new_start_token = _list_changes(drive, DRIVE_ID, next_page, 2)
    all_changes += changes

  # filter changes for relevant files for this project
  relevant_changed_file_ids = []
  for c in all_changes:
    file = _relevant_updated_file(c, FOLDER_ID)
    if file:
      relevant_changed_file_ids.append(file.get('id'))
      print(f"Detected changed file: {file.get('name')}")
  
  return relevant_changed_file_ids, new_start_token


def _fetch_all_files(drive, DRIVE_ID, FOLDER_ID):
  """
  Returns list of file_ids for all files in source drive folder + new start token
  - makes API calls to drive.files().list(), paginates for complete results
  - filters results for files only (excludes folders)
  - returns new start page token for subsequent incremental updates with changes API
  """  
  all_files, next = _list_files(drive, FOLDER_ID, 2)
  
  while next:
    files, next = _list_files(drive, FOLDER_ID, 2, next)
    all_files += files  
  
  start_token = _create_start_page_token(drive, DRIVE_ID)
  # filter for files only; not folders
  file_ids = []
  for file in all_files:
    if file['mimeType'] == "text/csv":
      file_ids.append(file["id"])

  return file_ids, start_token


def ingest_files(drive, s3, file_ids, BUCKET_NAME):
  """
  Loads each gdrive file in file_ids to s3
  Uses current date to build S3 target path
  """
  ingest_date = datetime.datetime.now().strftime("%Y-%m-%d")  # use ingest_date to build filepath

  print(f"Loading files to S3: {BUCKET_NAME}/raw/<dataset>/ingest-date={ingest_date}/<dataset>.csv")

  for file_id in file_ids:
    filename = _get_file_metadata(drive, file_id)["name"]
    dataset_name = Path(filename).stem
    path = f"raw/{dataset_name}.csv/ingest_date={ingest_date}/{dataset_name}.csv"
    
    # load drive file to s3
    print(f"Starting file transfer for: {filename}")
    buffer = _get_file_content_buffer(drive, file_id)
    s3.upload_fileobj(buffer, Key=path, Bucket=BUCKET_NAME)
    print(f"Uploaded {filename} to S3.\n")
  print("Ingested all files.")
  return


def _load_start_token(s3, bucket_name, path):

  response = s3.get_object(Bucket=bucket_name, Key=path)
  start_token = json.loads(response['Body'].read().decode('utf-8')).get('startPageToken')
  
  print(f"Loaded start token: {start_token} from s3://{bucket_name}/{path}")

  return start_token

def _save_start_page_token(s3, bucket_name, path, new_start_token):

  data = json.dumps({"startPageToken": new_start_token})
  s3.put_object(Body=data, Bucket=bucket_name, Key=path)
  
  print(f"Saved start token: {new_start_token} to s3://{bucket_name}/{path}")

  
def main(full_refresh=False, persist_state=False):
  """
  full_refresh - loads all files in source gdrive folder to s3
  persist_state - saves start page token to track state for subsequent incremental batch loads
  """
  print(f"main received args for persist_state: {persist_state}")
  
  mode = "Full Refresh" if full_refresh else "Incremental"
  print(f"Running ingest script in mode: {mode}")

  DRIVE_ID = os.environ["DRIVE_ID"]
  FOLDER_ID = os.environ["FOLDER_ID"]
  BUCKET_NAME = os.environ["BUCKET_NAME"]
  START_TOKEN_PATH = os.environ["START_TOKEN_PATH"]
  TEST_START_TOKEN_PATH = os.environ["TEST_START_TOKEN_PATH"]

  s3 = boto3.client('s3')
  drive = get_drive_client()

  if full_refresh:
    print(f"Fetching all files in full refresh mode")
    file_ids, new_start_token = _fetch_all_files(drive, DRIVE_ID, FOLDER_ID)
  
  else:
    start_token = _load_start_token(s3, BUCKET_NAME, START_TOKEN_PATH)
    print(f"Fetching all files in incremental mode based on start token: {start_token}")
    file_ids, new_start_token = _fetch_changed_files(drive, DRIVE_ID, FOLDER_ID, start_token)

    print(f"Found the following changed files and received new start token {new_start_token}")
    print(file_ids)
  
  ingest_files(drive, s3, file_ids, BUCKET_NAME)
  

  if persist_state:
    print("Persisting state...")
    _save_start_page_token(s3, BUCKET_NAME, START_TOKEN_PATH, new_start_token)
    print("Updated start page token for next incremental load")
  else:
    print("Not persisting state...")
  
  return
  