from urllib.parse import urlparse
import logging

from dataclasses import dataclass
from google.cloud.storage.client import Client


def path_has_any_data(bucket: str, prefix: str, gcs: Client) -> bool:
    """Checks the given path for any blobs of non-zero size"""
    blobs = [blob for blob in
             gcs.list_blobs(bucket, prefix=prefix)]
    return any([blob.size > 0 for blob in blobs])


def remove_empty_blobs(bucket: str, prefix: str, gcs: Client) -> None:
    """Checks the given path for any blobs of non-zero size"""
    blobs = [blob for blob in
             gcs.list_blobs(bucket, prefix=prefix)]
    for blob in blobs:
        if blob.size == 0:
            logging.info(f"Removing zero byte blob at {blob.name}")
            blob.delete()


@dataclass
class GsBucketWithPrefix:
    bucket: str
    prefix: str


def parse_gs_path(raw_gs_path: str) -> GsBucketWithPrefix:
    url_result = urlparse(raw_gs_path)
    return GsBucketWithPrefix(url_result.netloc, url_result.path[1:])
