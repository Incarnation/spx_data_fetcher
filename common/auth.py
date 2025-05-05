# common/auth.py
import json
import os
from functools import lru_cache

from google.oauth2 import service_account


@lru_cache()
def get_gcp_credentials():
    """
    Returns Google Cloud credentials.

    - If GOOGLE_SERVICE_ACCOUNT_JSON is set, parse it as JSON string.
    - If GOOGLE_APPLICATION_CREDENTIALS is set, load from file path.
    - Otherwise, raise an error.
    """
    json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    json_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if json_str:
        try:
            info = json.loads(json_str)
            return service_account.Credentials.from_service_account_info(info)
        except json.JSONDecodeError:
            raise ValueError("❌ GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON")

    elif json_path:
        return service_account.Credentials.from_service_account_file(json_path)

    raise EnvironmentError(
        "❌ Missing both GOOGLE_SERVICE_ACCOUNT_JSON and GOOGLE_APPLICATION_CREDENTIALS"
    )
