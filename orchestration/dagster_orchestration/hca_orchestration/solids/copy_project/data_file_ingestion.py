import base64

from dagster import solid
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)
from dagster_utils.contrib.data_repo.jobs import poll_job
from dagster_utils.contrib.data_repo.typing import JobId
from data_repo_client import JobModel
from google.cloud.storage.blob import Blob
from google.cloud.storage.bucket import Bucket

from hca_orchestration.contrib.gcs import parse_gs_path
from hca_orchestration.resources.config.hca_dataset import TargetHcaDataset
from hca_orchestration.resources.config.scratch import ScratchConfig
from hca_orchestration.solids.copy_project.subgraph_hydration import DataEntity


@solid(
    required_resource_keys={"gcs", "data_repo_client", "scratch_config", "target_hca_dataset"}
)
def ingest_data_files(context: AbstractComputeExecutionContext, data_entities: set[DataEntity]) -> None:
    storage_client = context.resources.gcs
    data_repo_client = context.resources.data_repo_client
    scratch_config: ScratchConfig = context.resources.scratch_config
    target_hca_dataset: TargetHcaDataset = context.resources.target_hca_dataset

    control_file_path = _generate_control_file(context, data_entities, scratch_config, storage_client)
    _bulk_ingest_to_tdr(context, control_file_path, data_repo_client, scratch_config, target_hca_dataset)


def _bulk_ingest_to_tdr(context, control_file_path, data_repo_client,
                        scratch_config: ScratchConfig, target_hca_dataset):
    payload = {
        "profileId": target_hca_dataset.billing_profile_id,
        "loadControlFile": f"gs://{scratch_config.scratch_bucket_name}/{control_file_path}",
        "loadTag": "arh_testing2",
        "maxFailedFileLoads": 0
    }
    context.log.info(f'Bulk file ingest payload = {payload}')
    job_response: JobModel = data_repo_client.bulk_file_load(
        target_hca_dataset.dataset_id,
        bulk_file_load=payload
    )
    job_id = JobId(job_response.id)
    context.log.info(f"Bulk file ingest submitted, polling on job_id = {job_id}")
    poll_job(job_id, 86400, 2, data_repo_client)


def _generate_control_file(context, data_entities: set[DataEntity], scratch_config: ScratchConfig, storage_client):
    ingest_items = []
    for data_entity in data_entities:
        file_bucket_and_prefix = parse_gs_path(data_entity.path)
        source_bucket = Bucket(storage_client, file_bucket_and_prefix.bucket)
        blob = Blob(file_bucket_and_prefix.prefix, source_bucket)
        blob.reload()

        target_path = f"{blob.name.split('/')[-1]}"
        ingest_items.append(
            f'{{"sourcePath":"{data_entity.path}", "targetPath":"/{data_entity.hca_file_id}/{target_path}"}}')

    control_file_str = "\n".join(ingest_items)
    bucket = storage_client.get_bucket(scratch_config.scratch_bucket_name)
    control_file_path = f"{scratch_config.scratch_prefix_name}/data_ingest_requests/control_file.txt"
    control_file_upload = bucket.blob(
        control_file_path
    )
    context.log.info(f"Uploading control file to gs://{scratch_config.scratch_bucket_name}/{control_file_path}")
    control_file_upload.upload_from_string(client=storage_client, data=control_file_str)
    return control_file_path
