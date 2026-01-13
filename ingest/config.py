from dataclasses import dataclass
import os

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