import base64
import re
from typing import cast, Optional, TypedDict, Union
from unittest.mock import create_autospec

from google.cloud import storage

BUCKET_PREFIX_REGEX = r'gs://[^/]+/(.*)$'


# not feature complete - new methods should be implemented as needed
class FakeGoogleBlob:
    def __init__(self, name, md5=None, content=None, require_reload_for_md5=False):
        self.name = name
        if require_reload_for_md5:
            self.md5_hash = None
        else:
            self.md5_hash = md5
        self._unloaded_md5 = md5
        self.content = content
        self.size = len((content or '').encode('utf-8'))

        # fake blob that mocks can reference to make sure mocked-out methods still demand the correct
        # call signature
        dummy_blob = storage.Blob('x', 'x')
        # mocks for tracking if functions that we want to be no-ops are called
        # they should be defined in the initializer so they're not shared across multiple instances
        self.upload_from_string = create_autospec(dummy_blob.upload_from_string)
        self.upload_from_file = create_autospec(dummy_blob.upload_from_file)

    def reload(self):
        self.md5_hash = self._unloaded_md5

    def download_to_file(self, file_object):
        file_object.write(self.content.encode('UTF8'))


class HexBlobInfo(TypedDict):
    hex_md5: str
    content: str


class Base64BlobInfo(TypedDict):
    md5: bytes
    content: str


BlobInfo = Union[HexBlobInfo, Base64BlobInfo]


class FakeGoogleBucket:
    _blobs: dict[str, FakeGoogleBlob]
    _nonexistent_blobs: dict[str, FakeGoogleBlob]

    # blob_info is a dictionary that maps blob paths to the hash of the metadata of that blob
    # Blob metadata can contain 3 fields (the first 2 of which are mutually exclusive):
    #     1) md5 (base64 format)
    #     2) hex_md5 (hexadecimal format)
    #     3) content (actual content of the file)
    def __init__(self, blob_info: dict[str, BlobInfo] = {}):
        self._blobs = {}
        self._nonexistent_blobs = {}

        for blob_uri, base_blob_metadata in blob_info.items():
            if blob_uri.startswith('gs://'):
                if match := re.match(BUCKET_PREFIX_REGEX, blob_uri):
                    blob_name = match.group(1)
                else:
                    raise ValueError(f"Invalid blob URI {blob_uri} provided.")
            else:
                blob_name = blob_uri

            if 'hex_md5' in base_blob_metadata:
                hex_metadata = cast(HexBlobInfo, base_blob_metadata)
                blob_metadata: Base64BlobInfo = {
                    'content': hex_metadata['content'],
                    'md5': base64.b64encode(bytes.fromhex(hex_metadata['hex_md5']))
                }

            self._blobs[blob_name] = FakeGoogleBlob(name=blob_name, **blob_metadata)

    def get_blob(self, path) -> Optional[FakeGoogleBlob]:
        return self._blobs.get(path)

    def blob(self, path) -> FakeGoogleBlob:
        if path not in self._nonexistent_blobs:
            self._nonexistent_blobs[path] = FakeGoogleBlob(name=path)

        return self._nonexistent_blobs[path]


class FakeGCSClient:
    def __init__(self, buckets: dict[str, FakeGoogleBucket] = {}):
        self._buckets = buckets

    def get_bucket(self, bucket_name):
        if bucket_name not in self._buckets:
            self._buckets[bucket_name] = FakeGoogleBucket()
        return self._buckets[bucket_name]

    def bucket(self, bucket_name):
        return self.get_bucket(bucket_name)

    def list_blobs(self, bucket, prefix=None):
        return self._buckets[bucket]._blobs.values()
