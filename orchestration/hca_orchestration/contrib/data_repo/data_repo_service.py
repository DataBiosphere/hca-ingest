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

    def find_dataset(self, dataset_name: str) -> Optional[TdrDataset]:
        result: EnumerateDatasetModel = self.data_repo_client.enumerate_datasets(filter=dataset_name)

        if result.filtered_total > 1:
            raise Exception(f"More than one match for dataset name {dataset_name}")

        if result.filtered_total == 0:
            return None

        dataset_summary: DatasetSummaryModel = result.items[0]
        return self.get_dataset(dataset_summary.id)

    def list_datasets(self, dataset_name: str) -> EnumerateDatasetModel:
        result: EnumerateDatasetModel = self.data_repo_client.enumerate_datasets(filter=dataset_name)
        return result

    def get_dataset(self, dataset_id: str) -> TdrDataset:
        dataset_model: DatasetModel = self.data_repo_client.retrieve_dataset(id=dataset_id)
        bq_location = self._get_dataset_bq_location(dataset_model)

        if not bq_location:
            raise ValueError(f"No bigquery location found for dataset {dataset_id}")

        return TdrDataset(dataset_model.name, dataset_model.id, dataset_model.data_project,
                          dataset_model.default_profile_id, bq_location)

    def _get_dataset_bq_location(self, dataset_model: DatasetModel) -> Optional[str]:
        for storage_info in dataset_model.storage:
            if storage_info.cloud_resource == 'bigquery':
                return str(storage_info.region)

        return None

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
        bq_location = self._get_dataset_bq_location(dataset_info)
        if not bq_location:
            raise ValueError(f"No bigquery location found for dataset {dataset_info.id}")

        return TdrDataset(
            dataset_info.name,
            dataset_info.id,
            dataset_info.data_project,
            dataset_info.default_profile_id,
            bq_location
        )
