from typing import Optional

import google.auth
from google.auth.transport.requests import AuthorizedSession, Request
from google.oauth2.credentials import Credentials

DEFAULT_SCOPES = ['openid', 'email', 'profile', 'https://www.googleapis.com/auth/cloud-platform']


def google_default() -> tuple[Credentials, str]:
    return google.auth.default(scopes=DEFAULT_SCOPES)


def get_credentials() -> Credentials:
    creds, _ = google_default()
    return creds


def default_google_access_token(credentials: Optional[Credentials] = None) -> str:
    credentials = credentials or get_credentials()
    credentials.refresh(Request())

    return credentials.token


def authorized_session() -> AuthorizedSession:
    return AuthorizedSession(get_credentials())
