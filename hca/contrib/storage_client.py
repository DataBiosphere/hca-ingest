from dataclasses import dataclass
from typing import Iterator
from urllib import parse

from dagster import resource, InitResourceContext
from google.cloud import storage
from google.cloud.storage import Client

from hca.contrib.google import authorized_session, google_default


def gs_path_from_bucket_prefix(bucket: str, prefix: str) -> str:
    return f"gs://{bucket}/{prefix}"


def path_has_any_data(bucket: str, prefix: str, gcs: Client) -> bool:
    """Checks the given path for any blobs of non-zero size"""
    blobs = [blob for blob in gcs.list_blobs(bucket, prefix=prefix)]
    return any([blob.size > 0 for blob in blobs])


def parse_gs_path(gs_path: str):
    split_url = parse.urlsplit(gs_path)
    if split_url.scheme != "gs" or not split_url.netloc:
        raise ValueError("Invalid GCS URL format. Expected format: gs://<bucket>/<path>")

    bucket = split_url.netloc
    prefix = split_url.path.lstrip("/") + "/"

    return type("GcsPath", (object,), {"bucket": bucket, "prefix": prefix})()


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
