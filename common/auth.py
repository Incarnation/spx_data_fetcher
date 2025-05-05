# common/auth.py
import json
import logging
import os

from google.oauth2 import service_account


def get_gcp_credentials():
    json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

    if not json_str:
        raise EnvironmentError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")

    info = json.loads(json_str)
    logging.info("✅ Google Cloud credentials loaded from environment.")
    return service_account.Credentials.from_service_account_info(info)
