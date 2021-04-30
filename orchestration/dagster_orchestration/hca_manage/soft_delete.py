import argparse
from dataclasses import dataclass, field
import logging
from typing import BinaryIO, Optional
import uuid

from data_repo_client import RepositoryApi, DataDeletionRequest
import google.auth.credentials
from google.cloud import storage

from hca_manage import __version__ as hca_manage_version
from hca_manage.common import data_repo_host, DefaultHelpParser, JobId, get_api_client, query_yes_no
from hca_orchestration.contrib import google as hca_google


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def run(arguments: Optional[list[str]] = None) -> None:
    parser = DefaultHelpParser(description="A simple CLI to soft delete rows in a TDR dataset.")
    parser.add_argument("-V", "--version", action="version", version="%(prog)s " + hca_manage_version)
    parser.add_argument("-e", "--env", help="The Jade environment to target", choices=["dev", "prod"], required=True)

    parser.add_argument("-p", "--path", help="Path to csv containing row IDs to soft delete")
    parser.add_argument("-t", "--target_table", help="Table containing the rows slated for soft deletion")
    parser.add_argument("-j", "--project", help="project")
    parser.add_argument("-d", "--dataset", help="dataset")

    args = parser.parse_args(arguments)
    host = data_repo_host[args.env]

    if query_yes_no("Are you sure?"):
        soft_delete(args, host)


def soft_delete(args: argparse.Namespace, host: str) -> None:
    hca = SoftDeleteManager(environment=args.env, data_repo_client=get_api_client(host=host),
                            project=args.project, dataset=args.dataset)
    hca.soft_delete_rows(args.path, args.target_table)


@dataclass
class SoftDeleteManager:
    environment: str
    data_repo_client: RepositoryApi
    project: Optional[str] = None
    dataset: Optional[str] = None

    bucket_project: str = field(init=False)
    bucket: str = field(init=False)

    def __post_init__(self) -> None:
        self.filename_template = f"sd-{self.project}-{self.dataset}-{uuid.uuid4()}-{{table}}.csv"
        self.bucket_project = {"prod": "mystical-slate-284720",
                               "dev": "broad-dsp-monster-hca-dev"}[self.environment]
        self.bucket = f"broad-dsp-monster-hca-{self.environment}-staging-storage"

    @property
    def gcp_creds(self) -> google.auth.credentials.Credentials:
        return hca_google.get_credentials()

    def soft_delete_rows(self, path: str, target_table: str) -> JobId:
        with open(path, mode="rb") as rf:
            remote_file_path = self.put_csv_in_bucket(local_file=rf, target_table=target_table)
            job_id = self._submit_soft_delete(target_table=target_table, target_path=remote_file_path)
            logging.info(f"Soft delete job for table {target_table} running, job id of: {job_id}")
            return job_id

    def put_csv_in_bucket(self, local_file: BinaryIO, target_table: str) -> str:
        """
        Puts a local file into a GCS bucket accessible by Jade.
        :param local_file: The file to upload.
        :param target_table: The table name with which to format the target filename.
        :return: The gs-path of the uploaded file.
        """
        storage_client = storage.Client(project=self.bucket_project, credentials=self.gcp_creds)
        bucket = storage_client.bucket(self.bucket)
        target_filename = self._format_filename(table=target_table)
        blob = bucket.blob(target_filename)
        blob.upload_from_file(local_file)

        filepath = f"gs://{self.bucket}/{target_filename}"
        logging.info(f"Put a soft-delete file here: {filepath}")

        return filepath

    def get_dataset_id(self) -> str:
        """
        Get the dataset ID of the provided dataset name.
        :return: The dataset id.
        """

        response = self.data_repo_client.enumerate_datasets(filter=self.dataset)
        return response.items[0].id  # type: ignore # data repo client has no type hints, since it's auto-generated

    def _format_filename(self, table: str) -> str:
        return self.filename_template.format(table=table)

    def _submit_soft_delete(self, target_table: str, target_path: str) -> JobId:
        """
        Submit a soft delete request.
        :param target_table: The table to apply soft deletion to.
        :param target_path: The gs-path of the csv that contains the row ids to soft delete.
        :return: The job id of the soft delete job.
        """
        dataset_id = self.get_dataset_id()

        response = self.data_repo_client.apply_dataset_data_deletion(
            id=dataset_id,
            data_deletion_request=DataDeletionRequest(
                delete_type="soft",
                spec_type="gcsFile",
                tables=[
                    {
                        "gcsFileSpec": {
                            "fileType": "csv",
                            "path": target_path
                        },
                        "tableName": target_table
                    }
                ]
            )
        )

        return response.id  # type: ignore # data repo client has no type hints, since it's auto-generated
