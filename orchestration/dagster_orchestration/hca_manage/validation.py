import json
import logging
from functools import cache
from typing import Optional, Any

import requests
from google.cloud import storage
from jsonschema import validate

from hca_manage.common import DefaultHelpParser
from hca_orchestration.contrib.gcs import parse_gs_path
from hca.staging_area_validator import StagingAreaValidator


@cache
def fetch_schema(path: str) -> Any:
    raw_schema = requests.get(path).json()
    return raw_schema


def validate_json(blob: storage.Blob) -> Optional[Exception]:
    """
    Validate that the JSON file blob follows the schema in the describedBy
    :param blob: JSON file blob to check
    :param schema_fetcher: Schema Fetcher that contains a set of unique schemas
    :return: An exception if JSON is invalid, return None if valid
    """
    try:
        data = json.loads(blob.download_as_string())
        schema_path = data["describedBy"]
        schema = fetch_schema(schema_path)
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
        if blob.name.endswith('json'):
            json_error = validate_json(blob)
            if json_error is None:
                valid_files_in_dir.append(blob.name)
            else:
                invalid_files_in_dir[blob.name] = json_error
        else:
            valid_files_in_dir.append(blob.name)
    if len(valid_files_in_dir) == 0 and len(invalid_files_in_dir) == 0:
        logging.error(f"{path} File Path doesn't exist")
    elif len(invalid_files_in_dir) > 0:
        logging.error(f"List of invalid json files {invalid_files_in_dir}")
    else:
        logging.info('File path and Json are valid')


def validate_staging_area(path: str, ignore_inputs: bool) -> None:
    """
    Run the UCSC pre-checks on the staging area to identify potential snapshot or indexing failures
    :param path: Google stage path name
    """
    adapter = StagingAreaValidator(
        staging_area=path,
        ignore_dangling_inputs=ignore_inputs,
        validate_json=True
    )
    exit_code = adapter.main()
    if exit_code is None:
        logging.info('Staging area is valid')
    else:
        logging.error('Staging area is invalid')


def run(arguments: Optional[list[str]] = None) -> None:
    parser = DefaultHelpParser(description="CLI to manage validate GS path and json files.")
    parser.add_argument("-p", "--path", help="GS path to validate", required=True)
    parser.add_argument("-p", "--ignore_inputs", help="Ignore input metadata files", default=False)
    args = parser.parse_args(arguments)

    storage_client = storage.Client()
    gs_bucket = parse_gs_path(args.path)
    bucket = storage_client.bucket(gs_bucket.bucket)

    well_known_dirs = {'/data', '/descriptors', '/links', '/metadata'}
    validate_staging_area(args.path, args.ignore_inputs)

    for dir in well_known_dirs:
        validate_directory(gs_bucket.prefix + dir, bucket)


if __name__ == "__main__":
    run()
