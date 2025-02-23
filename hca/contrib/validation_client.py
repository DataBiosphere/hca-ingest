import json
import logging
import uuid
from functools import cached_property, lru_cache
from typing import MutableMapping, MutableSequence, Optional, Tuple, TypeVar
from urllib import parse

import google.cloud.storage as gcs
import requests
from google.cloud.storage import Client
from jsonschema import FormatChecker, validate
from requests.adapters import HTTPAdapter, Retry

from hca.contrib.storage_client import parse_gs_path

T = TypeVar("T")
JSON = MutableMapping[str, T]

log = logging.getLogger(__name__)

staging_area_properties_schema = {
    "$schema": "https://json-schema.org/draft/2019-09/schema",
    "properties": {"is_delta": {"type": "boolean"}},
    "required": ["is_delta"],
    "additionalProperties": False,
}


class StagingAreaValidator:
    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"

    def __init__(
            self,
            staging_area: str,
            ignore_dangling_inputs: bool,
            validate_json: bool,
            total_retries: int = 10,
    ) -> None:
        self.staging_area = staging_area
        self.validate_json = validate_json
        self.ignore_dangling_inputs = ignore_dangling_inputs
        self.total_retries = total_retries

        self.gcs = gcs.Client()
        self.is_delta = None
        self.names_to_id: MutableMapping[str, str] = {}
        self.metadata_files: MutableMapping[str, JSON] = {}
        self.file_errors: MutableMapping[str, Exception] = {}
        self.extra_files: MutableSequence[str] = []

        self.bucket, self.sa_path = self._parse_gcs_url(self.staging_area)

    @cached_property
    def validator(self):
        return SchemaValidator()

    def _parse_gcs_url(self, gcs_url: str) -> Tuple[gcs.Bucket, str]:
        split_url = parse.urlsplit(gcs_url)
        if split_url.scheme != "gs" or not split_url.netloc:
            log.error("Invalid GCS URL format: gs://<bucket>[/<path>]")
            exit(1)
        path = split_url.path.lstrip("/") + "/" if split_url.path else ""
        return gcs.Bucket(self.gcs, split_url.netloc), path

    def main(self):
        self._run()
        exit_code = 0
        for file_name, e in self.file_errors.items():
            log.error(f"Error with file: {file_name}", exc_info=e)
        for file_name in self.extra_files:
            log.warning(f"File not part of a subgraph: {file_name}")
        if self.file_errors:
            exit_code |= 1
            log.error(f"Encountered {len(self.file_errors)} files with errors")
        return exit_code

    def _run(self):
        self.file_errors.clear()
        self.validate_staging_area_properties()
        if not self.file_errors:
            for category in ("links", "metadata", "descriptors", "data"):
                self.validate_files(category)
            self.check_results()

    def validate_staging_area_properties(self):
        log.info("Checking staging area properties")
        properties_file_path = self.sa_path + "staging_area.json"
        blob = self.bucket.get_blob(properties_file_path)
        assert isinstance(blob, gcs.Blob), properties_file_path
        file_json = self.download_blob_as_json(blob)
        self.validate_file_json(file_json, blob.name, staging_area_properties_schema)
        self.is_delta = file_json["is_delta"]

    def validate_files(self, path: str):
        log.info(f"Checking files in {self.sa_path}{path}")
        validate_fn = getattr(self, f"validate_{path}_file")
        for blob in self.bucket.list_blobs(prefix=f"{self.sa_path}{path}"):
            try:
                validate_fn(blob)
            except KeyboardInterrupt:
                exit()
            except Exception as e:
                log.error(f"File error: {blob.name}")
                self.file_errors[blob.name] = e

    def download_blob_as_json(self, blob: gcs.Blob) -> Optional[JSON]:
        return json.loads(blob.download_as_bytes())

    def validate_links_file(self, blob: gcs.Blob):
        _, _, file_name = blob.name.rpartition("/")
        assert file_name.count("_") == 2 and file_name.endswith(".json")
        _, _, project_uuid = file_name[:-5].split("_")
        file_json = self.download_blob_as_json(blob)
        self.validate_file_json(file_json, blob.name)

        for link in file_json["links"]:
            if link["link_type"] == "process_link":
                self.add_metadata_file(link["process_id"], link["process_type"], project_uuid, "process")
                for category in ("input", "output", "protocol"):
                    for entity in link[f"{category}s"]:
                        self.add_metadata_file(entity[f"{category}_id"], entity[f"{category}_type"], project_uuid,
                                               category)
            elif link["link_type"] == "supplementary_file_link":
                assert link["entity"]["entity_type"] == "project"
                assert link["entity"]["entity_id"] == project_uuid
                for entity in link["files"]:
                    self.add_metadata_file(entity["file_id"], entity["file_type"], project_uuid, "supplementary")

        if project_uuid not in self.metadata_files:
            self.add_metadata_file(project_uuid, "project", project_uuid, "project")

    def add_metadata_file(self, entity_id: str, entity_type: str, project_uuid: str, category: str):
        metadata = self.metadata_files.setdefault(entity_id, {
            "name": set(),
            "entity_id": entity_id,
            "entity_type": entity_type,
            "metadata_versions": set(),
            "descriptor_versions": set(),
            "project": {project_uuid},
            "category": {category},
            "found_metadata": False,
        })
        metadata["project"].add(project_uuid)
        metadata["category"].add(category)

    def validate_metadata_file(self, blob: gcs.Blob):
        metadata_type, metadata_file = blob.name.split("/")[-2:]
        assert metadata_file.count("_") == 1 and metadata_file.endswith(".json")
        metadata_id, metadata_version = metadata_file[:-5].split("_")
        file_json = self.download_blob_as_json(blob)
        self.validate_file_json(file_json, blob.name)

        metadata = self.metadata_files.setdefault(metadata_id, {})
        metadata["name"].add(blob.name)
        metadata["metadata_versions"].add(metadata_version)
        metadata["found_metadata"] = True

    def validate_file_json(self, file_json: JSON, file_name: str, schema: Optional[JSON] = None):
        if self.validate_json:
            log.info(f"Validating JSON of {file_name}")
            try:
                self.validator.validate_json(file_json, self.total_retries, schema)
            except Exception as e:
                log.error(f"File {file_name} failed JSON validation.")
                self.file_errors[file_name] = e

    def check_results(self):
        log.info("Checking results")
        for metadata_id, metadata_file in self.metadata_files.items():
            try:
                self.check_result(metadata_file)
            except Exception as e:
                log.error(f"File error: {metadata_file}")
                self.file_errors[metadata_id] = e
        if not self.file_errors and not self.extra_files:
            log.info("No errors found")

    def check_result(self, metadata_file):
        if not metadata_file["found_metadata"]:
            if metadata_file["entity_type"] == "project":
                log.warning(f"Missing metadata file for project {metadata_file['project']}")
            else:
                raise Exception("Missing metadata file", metadata_file)

    def validate_uuid(self, value: str):
        try:
            uuid.UUID(value)
        except ValueError as e:
            raise ValueError("Invalid UUID value", value) from e


class SchemaValidator:
    @classmethod
    def validate_json(cls, file_json: JSON, total_retries: int, schema: Optional[JSON] = None):
        schema = schema or cls._download_schema(file_json["describedBy"], total_retries)
        validate(file_json, schema, format_checker=FormatChecker())

    @classmethod
    # setting to maxsize=None so as not to evict old values, and maybe help avoid connectivity issues (DI-22)
    @lru_cache(maxsize=None)
    def _download_schema(cls, schema_url: str, total_retries: int) -> JSON:
        log.debug("Downloading schema %s", schema_url)

        s = requests.Session()
        log.debug(f"total_retries = {total_retries}")
        retries = Retry(
            total=total_retries,
            backoff_factor=0.2,
            status_forcelist=[500, 502, 503, 504],
        )
        s.mount("http://", HTTPAdapter(max_retries=retries))
        s.mount("https://", HTTPAdapter(max_retries=retries))

        response = s.get(schema_url, allow_redirects=False)
        response.raise_for_status()
        return response.json()


class HcaValidator:

    def validate_staging_area(self, path: str, ignore_inputs: bool, client: Client) -> int:
        validator = StagingAreaValidator(
            staging_area=path,
            ignore_dangling_inputs=ignore_inputs,
            validate_json=True,
        )
        exit_code = validator.main()

        if exit_code == 0:
            logging.info(f"Staging area {path} is valid")
        else:
            logging.error(f"Staging area {path} is invalid")

        return exit_code

    def validate_structure(self, path: str, gs_client: Client) -> int:
        required_dirs = {"/data", "/descriptors", "/links", "/metadata"}
        bucket_with_prefix = parse_gs_path(path)
        bucket = gs_client.bucket(bucket_with_prefix.bucket)

        exit_code = 0
        for directory in required_dirs:
            expected_path = f"{bucket_with_prefix.prefix}{directory}"
            if not list(gs_client.list_blobs(bucket, prefix=expected_path)):
                logging.error(f"Missing expected directory: {directory} at {path}")
                exit_code = 1

        return exit_code


class LocalHcaValidator:
    def validate_staging_area(self, path: str, ignore_inputs: bool, client: Client) -> int:
        return 0

    def validate_structure(self, path: str, gs_client: Client) -> int:
        return 0
