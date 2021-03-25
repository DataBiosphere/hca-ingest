import google.auth
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials


def get_credentials() -> Credentials:
    creds, _ = google.auth.default()
    return creds


def default_google_access_token() -> str:
    # get token for google-based auth use, assumes application default credentials work for specified environment
    credentials = get_credentials()
    credentials.refresh(Request())

    return credentials.token  # type: ignore # (unannotated library)
