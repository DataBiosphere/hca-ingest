
import data_repo_client
from google.auth.transport.requests import Request
from google.auth.credentials import Credentials


class RefreshingAccessTokenConfig(data_repo_client.Configuration):
    """
    Helper class that hooks access to the access_token property so we can
    intercept and potentially refresh if expired
    """

    def __init__(self, host: str, google_credentials: Credentials) -> None:
        super().__init__(host)
        self._google_credentials = google_credentials

    @property
    def access_token(self) -> str:
        # if creds are expired, attempt to refresh
        if not self._google_credentials.valid:
            self._google_credentials.refresh(Request())
        self._access_token: str = self._google_credentials.token
        return self._access_token

    @access_token.setter
    def access_token(self, value: str) -> None:
        self._access_token = value