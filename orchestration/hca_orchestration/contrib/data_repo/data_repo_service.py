"""
Abstraction over the raw TDR data repo client. All async job operations automatically poll on the returned job
to completion.
"""
import logging
from dataclasses import dataclass
from typing import Optional

from dagster_utils.contrib.data_repo.jobs import poll_job
from dagster_utils.contrib.data_repo.typing import JobId
from data_repo_client import RepositoryApi, JobModel, EnumerateDatasetModel, DatasetSummaryModel, DatasetModel

from hca_manage.dataset import DatasetManager
from hca_orchestration.models.hca_dataset import TdrDataset


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

    def find_dataset(self, dataset_name: str, env: str) -> Optional[TdrDataset]:
        result: EnumerateDatasetModel = self.data_repo_client.enumerate_datasets(filter=dataset_name)

        if result.filtered_total > 1:
            raise Exception(f"More than one match for dataset name {dataset_name}")

        if result.filtered_total == 0:
            return None

        dataset_summary: DatasetSummaryModel = result.items[0]
        dataset_manager = DatasetManager(env, self.data_repo_client)
        dataset_model = dataset_manager.retrieve_dataset(dataset_summary.id)
        return TdrDataset(dataset_model.name, dataset_model.id,
                          dataset_model.data_project, dataset_model.default_profile_id)

    def create_dataset(
            self,
            dataset_name: str,
            tdr_env: str,
            billing_profile_id: str,
            policy_members: set[str],
            region: str,
            description: str) -> TdrDataset:
        dataset_manager = DatasetManager(tdr_env, self.data_repo_client)
        dataset_info = dataset_manager.create_dataset_with_policy_members(
            dataset_name,
            billing_profile_id,
            policy_members,
            dataset_manager.generate_schema(),
            region,
            tdr_env,
            description
        )
        return TdrDataset(
            dataset_info.name,
            dataset_info.id,
            dataset_info.data_project,
            dataset_info.default_profile_id
        )
