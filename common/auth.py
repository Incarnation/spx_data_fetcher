# common/auth.py
import json
import os
from functools import lru_cache

from google.oauth2 import service_account


@lru_cache()
def get_gcp_credentials():
    json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not json_str:
        raise EnvironmentError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")

    try:
        info = json.loads(json_str)
        return service_account.Credentials.from_service_account_info(info)
    except json.JSONDecodeError:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON")
