from google.cloud import storage
import logging
from jsonschema import validate
import json
import requests
from typing import Optional
from hca_manage.common import DefaultHelpParser


def validate_json(blob: storage.Blob) -> Optional[Exception]:
    """
    Validate that the JSON file blob follows the schema in the describedBy
    :param blob: JSON file blob to check
    :return: An exception if JSON is invalid, return None if valid
    """
    try:
        data = json.loads(blob.download_as_string(client=None))
        schema_path = data["describedBy"]
        raw_schema = requests.get(schema_path).json()
        validate(instance=data, schema=raw_schema)
        return None
    except Exception as e:
        return e


def validate_directory(path: str, bucket: storage.Client.bucket):
    """
    Validate that the directory in the Google storage bucket is valid
    :param path: Google stage path name
    :param bucket: Google storage bucket name
    """
    valid_files_in_dir = []
    invalid_files_in_dir = {}
    for blob in bucket.list_blobs(prefix=path):
        # bucket_file = bucket.blob(blob.name)
        json_error = validate_json(blob)
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


def run(arguments: Optional[list[str]] = None):
    parser = DefaultHelpParser(description="CLI to manage validate GS path and json files.")
    parser.add_argument("-p", "--path", help="GS path to validate", required=True)
    parser.add_argument("-b", "--bucket", help="Bucket the GS path exists in", required=True)

    args = parser.parse_args(arguments)

    storage_client = storage.Client()
    bucket = storage_client.bucket(args.bucket)

    validate_directory(path=args.path + '/data', bucket=bucket)
    validate_directory(path=args.path + '/descriptors', bucket=bucket)
    validate_directory(path=args.path + '/links', bucket=bucket)
    validate_directory(path=args.path + '/metadata', bucket=bucket)


run()
