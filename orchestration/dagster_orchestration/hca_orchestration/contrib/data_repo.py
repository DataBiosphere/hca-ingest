"""
Abstraction over the raw TDR data repo client. All async job operations automatically poll on the returned job
to completion.
"""
import logging
from dataclasses import dataclass

from dagster_utils.contrib.data_repo.jobs import poll_job
from dagster_utils.contrib.data_repo.typing import JobId
from data_repo_client import RepositoryApi, JobModel


@dataclass
class DataRepoService:
    data_repo_client: RepositoryApi

    def delete_data(self, dataset_id: str, control_file_path: str, table_name: str) -> JobId:
        payload = {
            "deleteType": "soft",
            "specType": "gcsFile",
            "tables": [
                {
                    "gcsFileSpec": {
                        "fileType": "csv",
                        "path": control_file_path
                    },
                    "tableName": table_name
                }
            ]
        }

        job_response: JobModel = self.data_repo_client.apply_dataset_data_deletion(
            id=dataset_id,
            data_deletion_request=payload
        )

        job_id = JobId(job_response.id)
        logging.info(f"Polling on job_id = {job_id}")
        poll_job(job_id, 600, 2, self.data_repo_client)
        return job_id

    def ingest_data(self, dataset_id: str, control_file_path: str, table_name: str) -> JobId:
        payload = {
            "format": "json",
            "ignore_unknown_values": "false",
            "max_bad_records": 0,
            "path": control_file_path,
            "table": table_name
        }
        job_response: JobModel = self.data_repo_client.ingest_dataset(
            id=dataset_id,
            ingest=payload
        )

        job_id = JobId(job_response.id)
        logging.info(f"Polling on job_id = {job_id}")
        poll_job(job_id, 600, 2, self.data_repo_client)
        return job_id
