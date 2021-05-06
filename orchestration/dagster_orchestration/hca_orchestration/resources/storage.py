from typing import Iterator

from dagster import resource
from dagster.core.execution.context.init import InitResourceContext

import google.auth
from google.cloud import storage
from unittest.mock import create_autospec


@resource
def google_storage_client(init_context: InitResourceContext) -> storage.Client:
    credentials, project = google.auth.default()

    return storage.Client(project=project, credentials=credentials)


class MockBlob:
    def __init__(self, name: str):
        self.name = name

    def delete(self) -> None:
        pass


class LocalStorageClient:
    def list_blobs(self, bucket_name: str, prefix: str) -> Iterator[MockBlob]:
        for i in range(0, 10):
            yield MockBlob(f"fake_blob_{i}")


@resource
def local_storage_client(init_context: InitResourceContext) -> LocalStorageClient:
    return LocalStorageClient()
