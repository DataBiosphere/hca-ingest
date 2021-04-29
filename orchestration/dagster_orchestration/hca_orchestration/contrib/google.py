import google.auth
from google.auth.transport.requests import AuthorizedSession, Request
from google.oauth2.credentials import Credentials

DEFAULT_SCOPES = ['openid', 'email', 'profile', 'https://www.googleapis.com/auth/cloud-platform']


def get_credentials() -> Credentials:
    creds, _ = google.auth.default(scopes=DEFAULT_SCOPES)
    return creds


def default_google_access_token() -> str:
    # get token for google-based auth use, assumes application default credentials work for specified environment
    credentials = get_credentials()
    credentials.refresh(Request())

    return credentials.token  # type: ignore # (unannotated library)


def authorized_session() -> AuthorizedSession:
    return AuthorizedSession(default_google_access_token())
