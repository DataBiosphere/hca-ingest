from google.cloud import storage
import logging
from jsonschema import validate
import json
import requests
from typing import Optional
from hca_manage.common import DefaultHelpParser
from urllib.parse import urlparse
from dataclasses import dataclass


class SchemaFetcher:
    def __init__(self) -> None:
        self._schema_cache = {}

    def fetch_schema(self, path: str) -> json:
        if path in self._schema_cache:
            return self._schema_cache[path]

        raw_schema = requests.get(path).json()
        self._schema_cache[path] = raw_schema
        return raw_schema


@dataclass
class GsBucketWithPrefix:
    bucket: str
    prefix: str


def parse_gs_path(raw_gs_path: str) -> GsBucketWithPrefix:
    url_result = urlparse(raw_gs_path)
    return GsBucketWithPrefix(url_result.netloc, url_result.path[1:])


def validate_json(blob: storage.Blob, schema_fetcher: Type[SchemaFetcher]) -> Optional[Exception]:
    """
    Validate that the JSON file blob follows the schema in the describedBy
    :param blob: JSON file blob to check
    :param schema_fetcher: Schema Fetcher that contains a set of unique schemas
    :return: An exception if JSON is invalid, return None if valid
    """
    try:
        data = json.loads(blob.download_as_string())
        schema_path = data["describedBy"]
        schema = schema_fetcher.fetch_schema(schema_path)
        validate(instance=data, schema=schema)
        return None
    except Exception as e:
        return e


def validate_directory(path: str, bucket: storage.Client.bucket) -> None:
    """
    Validate that the directory in the Google storage bucket is valid
    :param path: Google stage path name
    :param bucket: Google storage bucket name
    """
    valid_files_in_dir = []
    invalid_files_in_dir = {}
    for blob in bucket.list_blobs(prefix=path):
        json_error = validate_json(blob, SchemaFetcher)
        if json_error is None:
            valid_files_in_dir.append(blob.name)
        else:
            invalid_files_in_dir[blob.name] = json_error
    if len(valid_files_in_dir) == 0 and len(invalid_files_in_dir) == 0:
        logging.error(f"{path} File Path doesn't exist")
    elif len(invalid_files_in_dir) > 0:
        logging.error(f"List of invalid json files {invalid_files_in_dir}")
    else:
        logging.info('File path and Json are valid')


def run(arguments: Optional[list[str]] = None) -> None:
    parser = DefaultHelpParser(description="CLI to manage validate GS path and json files.")
    parser.add_argument("-p", "--path", help="GS path to validate", required=True)
    args = parser.parse_args(arguments)

    storage_client = storage.Client()
    gs_bucket = parse_gs_path(args.path)
    bucket = storage_client.bucket(gs_bucket.bucket)

    well_known_dirs = {'/data', '/descriptors', '/links', '/metadata'}
    for dir in well_known_dirs:
        validate_directory(gs_bucket.prefix + dir, bucket)


if __name__ == "__main__":
    run()
