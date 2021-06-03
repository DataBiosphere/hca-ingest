from google.cloud.storage.client import Client


def path_has_any_data(bucket: str, prefix: str, gcs: Client) -> bool:
    """Checks the given path for any blobs of non-zero size"""
    blobs = [blob for blob in
             gcs.list_blobs(bucket, prefix=prefix)]
    return any([blob.size > 0 for blob in blobs])
