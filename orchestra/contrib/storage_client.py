from dataclasses import dataclass
from typing import Iterator
from urllib.parse import urlparse

from dagster import resource, InitResourceContext
from google.cloud import storage
from google.cloud.storage import Client

from orchestra.contrib.google import authorized_session, google_default


def gs_path_from_bucket_prefix(bucket: str, prefix: str) -> str:
    return f"gs://{bucket}/{prefix}"


def path_has_any_data(bucket: str, prefix: str, gcs: Client) -> bool:
    """Checks the given path for any blobs of non-zero size"""
    blobs = [blob for blob in
             gcs.list_blobs(bucket, prefix=prefix)]
    return any([blob.size > 0 for blob in blobs])


@dataclass
class GsBucketWithPrefix:
    bucket: str
    prefix: str

    def to_gs_path(self) -> str:
        return f"gs://{self.bucket}/{self.prefix}"


def parse_gs_path(raw_gs_path: str) -> GsBucketWithPrefix:
    if not raw_gs_path.startswith("gs://"):
        raise ValueError("GS path must being with gs:// scheme")
    url_result = urlparse(raw_gs_path)
    return GsBucketWithPrefix(url_result.netloc, url_result.path[1:])


@resource
def google_storage_client(_: InitResourceContext) -> storage.Client:
    _, project = google_default()

    return storage.Client(project=project, _http=authorized_session())


@dataclass
class MockBlob:
    name: str
    size: int

    def delete(self) -> None:
        pass


class MockStorageClient:
    def list_blobs(self, bucket_name: str, prefix: str) -> Iterator[MockBlob]:
        for i in range(0, 10):
            yield MockBlob(f"fake_blob_{i}", 1)


@resource
def local_google_storage_client(_: InitResourceContext) -> MockStorageClient:
    return MockStorageClient()
