import io
import json
import os
from dataclasses import dataclass

import boto3
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ============================================================================
# CONFIG CLASS 
# ============================================================================

@dataclass(frozen=True)
class AppConfig:
    drive_id: str
    folder_id: str
    bucket_name: str
    start_token_path: str
    oauth_secret_name: str

def load_config():
    return AppConfig(
        drive_id=os.environ["DRIVE_ID"],
        folder_id=os.environ["FOLDER_ID"],
        bucket_name=os.environ["BUCKET_NAME"],
        start_token_path=os.environ["START_TOKEN_PATH"],
        oauth_secret_name=os.environ["OAUTH_SECRET_NAME"],
    )

# ============================================================================
# Secrets / Auth
# ============================================================================

def load_secret(secret_name) -> dict:
  """
  Load a secret stored in Secrets Manager as SecretString and parse it as JSON
  """  
  secrets = boto3.client('secretsmanager')
  secret_str = secrets.get_secret_value(SecretId=secret_name)['SecretString']
  return json.loads(secret_str)
  

def save_secret_str(secret_name, data: str):
  """
  Store a new secret value (SecretString) in Secrets Manager
  """  
  secrets = boto3.client('secretsmanager')
  secrets.put_secret_value(SecretId=secret_name, SecretString=data)  # stored as JSON string


# ============================================================================
# Google Drive Client
# ============================================================================

def get_drive_client():
  """
  Create a Google Drive API v3 client

  Loads OAuth token JSON from Secrets Manager; refreshes and saves refreshed token if it is expired
  """
  c = load_config()
  SECRET_NAME = c.oauth_secret_name
  token = load_secret(SECRET_NAME)
  creds = Credentials.from_authorized_user_info(token)

  if creds.expired and creds.refresh_token: 
    creds.refresh(Request())
    print("Refreshed token")
    save_secret_str(SECRET_NAME, creds.to_json())
    print("Updated secret with refreshed token")
  else:
    print("Using token loaded from secret")    
  drive = build('drive', 'v3', credentials=creds)
  print("Created drive service")
  return drive

# ============================================================================
# S3 Client
# ============================================================================

def get_s3_client():
  s3 = boto3.client('s3')
  return s3

# ============================================================================
# Load JSON from S3
# ============================================================================

def load_start_token(s3, bucket_name, path):

  response = s3.get_object(Bucket=bucket_name, Key=path)
  start_token = json.loads(response['Body'].read().decode('utf-8')).get('startPageToken')
  
  print(f"Loaded start token: {start_token} from s3://{bucket_name}/{path}")
  return start_token

def save_start_page_token(s3, bucket_name, path, new_start_token):

  data = json.dumps({"startPageToken": new_start_token})
  s3.put_object(Body=data, Bucket=bucket_name, Key=path)
  print(f"Saved start token: {new_start_token} to s3://{bucket_name}/{path}")

# ============================================================================
# Google Drive API helpers
# ============================================================================

def list_files(drive, parent_folder, page_size=None, page_token=None, fields=None):
  """
  List items directly under a Drive folder
  """  
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

def create_start_page_token(drive, DRIVE_ID):
  results = drive.changes().getStartPageToken(
    driveId=DRIVE_ID,
    supportsAllDrives=True
  ).execute()
  return results.get("startPageToken")

def list_changes(drive, DRIVE_ID, page_token, page_size=None, fields=None):
  """
  List change records from Drive's account-wide Change stream
  """  
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

def get_file_metadata(drive, file_id, fields=None):
  results = drive.files().get(
    fileId=file_id,
    supportsAllDrives=True,
    fields=fields if fields else "id,name,mimeType,modifiedTime,trashed,md5Checksum,size"
  ).execute()
  return results


def get_file_content_buffer(drive, file_id):
  """
  Download a Drive file into an in-memory BytesIO buffer using MediaIoBaseDownload
  """  
  buffer = io.BytesIO()
  request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)

  downloader = MediaIoBaseDownload(fd=buffer, request=request)
  done = False
  while not done: 
    status, done = downloader.next_chunk()
    if status: 
      print(f"Download {int(status.progress() * 100)}%")

  buffer.seek(0)
  return buffer