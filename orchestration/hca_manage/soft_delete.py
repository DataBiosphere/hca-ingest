import argparse
import logging
import uuid
from dataclasses import dataclass, field
from typing import BinaryIO, Optional

import google.auth.credentials
from dagster_utils.contrib import google as hca_google
from dagster_utils.contrib.data_repo.jobs import poll_job
from dagster_utils.contrib.data_repo.typing import JobId
from data_repo_client import DataDeletionRequest, RepositoryApi
from google.cloud import storage

from hca_manage import __version__ as hca_manage_version
from hca_manage.common import (
    DefaultHelpParser,
    data_repo_host,
    get_api_client,
    get_dataset_id,
    query_yes_no,
    setup_cli_logging_format,
)


def run(arguments: Optional[list[str]] = None) -> None:
    setup_cli_logging_format()

    parser = DefaultHelpParser(description="A simple CLI to soft delete rows in a TDR dataset.")
    parser.add_argument("-V", "--version", action="version", version="%(prog)s " + hca_manage_version)
    parser.add_argument("-e", "--env", help="The Jade environment to target",
                        choices=["dev", "prod", "real_prod"], required=True)

    parser.add_argument("-p", "--path", help="Path to csv containing row IDs to soft delete")
    parser.add_argument("-t", "--target_table", help="Table containing the rows slated for soft deletion")
    parser.add_argument("-j", "--project", help="project")
    parser.add_argument("-d", "--dataset", help="dataset")

    args = parser.parse_args(arguments)
    host = data_repo_host[args.env]

    if query_yes_no("Are you sure?"):
        soft_delete(args, host)


def soft_delete(args: argparse.Namespace, host: str) -> None:
    hca = SoftDeleteManager(environment=args.env,
                            data_repo_client=get_api_client(host=host),
                            project=args.project,
                            dataset=args.dataset)
    hca.soft_delete_rows(args.path, args.target_table)


@dataclass
class SoftDeleteManager:
    environment: str
    data_repo_client: RepositoryApi
    project: str
    dataset: str

    bucket_project: str = field(init=False)
    bucket: str = field(init=False)

    def __post_init__(self) -> None:
        self.filename_template = f"sd-{self.project}-{self.dataset}-{uuid.uuid4()}-{{table}}.csv"
        self.bucket_project = {"prod": "mystical-slate-284720",
                               "real_prod": "mystical-slate-284720",
                               "dev": "broad-dsp-monster-hca-dev"}[self.environment]
        self.bucket = f"broad-dsp-monster-hca-prod-staging-storage"

    @property
    def gcp_creds(self) -> google.auth.credentials.Credentials:
        return hca_google.get_credentials()

    def soft_delete_rows(self, path: str, target_table: str) -> JobId:
        with open(path, mode="rb") as rf:
            remote_file_path = self.put_soft_delete_csv_in_bucket(local_file=rf, target_table=target_table)
            job_id = self._submit_soft_delete(target_table=target_table, target_path=remote_file_path)
            logging.info(f"Soft delete job for table {target_table} running, job id of: {job_id}")
            poll_job(job_id, 600, 2, self.data_repo_client)
            return job_id

    def put_soft_delete_csv_in_bucket(self, local_file: BinaryIO, target_table: str) -> str:
        """
        Puts a local file into a GCS bucket accessible by Jade so that a soft delete operation can be performed.
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

    def _format_filename(self, table: str) -> str:
        return self.filename_template.format(table=table)

    def _submit_soft_delete(self, target_table: str, target_path: str) -> JobId:
        """
        Submit a soft delete request.
        :param target_table: The table to apply soft deletion to.
        :param target_path: The gs-path of the csv that contains the row ids to soft delete.
        :return: The job id of the soft delete job.
        """
        dataset_id = get_dataset_id(dataset=self.dataset, data_repo_client=self.data_repo_client)

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

        return JobId(response.id)  # type: ignore # data repo client has no type hints, since it's auto-generated


if __name__ == '__main__':
    run()
