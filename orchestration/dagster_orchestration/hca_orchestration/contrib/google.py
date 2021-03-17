import google.auth
from google.auth.transport.requests import Request


def default_google_access_token() -> str:
    # get token for google-based auth use, assumes application default credentials work for specified environment
    credentials, _ = google.auth.default()
    credentials.refresh(Request())

    return credentials.token  # type: ignore # (unannotated library)
