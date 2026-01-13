"""
Entry point for local development.
Calls lambda_handler() from lambda_function.py.
"""

import os
from lambda_function import lambda_handler
from dotenv import load_dotenv, find_dotenv, set_key


if __name__ == "__main__":

  load_dotenv(override=True)

  lambda_handler(event = {"full_refresh": False, "persist_state": True}, context={})
  