from dagster import resource

import google.auth
from google.cloud import storage


@resource
def google_storage_client(init_context):
    credentials, project = google.auth.default()

    return storage.Client(project=project, credentials=credentials)


@resource
def local_storage_client(init_context):
    class MockBlob:
        def delete(self):
            pass

    class LocalStorageClient:
        def list_blobs(self, bucket_name: str, prefix: str):
            for _ in range(0, 10):
                yield MockBlob()

    return LocalStorageClient()
