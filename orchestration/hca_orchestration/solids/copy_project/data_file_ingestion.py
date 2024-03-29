from dagster import solid, op, Field, Failure
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)
from dagster_utils.contrib.data_repo.jobs import poll_job
from dagster_utils.contrib.data_repo.typing import JobId
from data_repo_client import JobModel, RepositoryApi
from google.cloud.storage import Client
from google.cloud.storage.blob import Blob
from google.cloud.storage.bucket import Bucket
from hca_orchestration.models.hca_dataset import TdrDataset

from hca_orchestration.contrib.gcs import parse_gs_path
from hca_orchestration.models.scratch import ScratchConfig
from hca_orchestration.solids.copy_project.subgraph_hydration import DataFileEntity
from hca_orchestration.solids.load_hca.poll_ingest_job import DataFileIngestionFailure


@op(
    required_resource_keys={
        "gcs",
        "data_repo_client",
        "scratch_config",
        "target_hca_dataset",
        "load_tag"
    },
    config_schema={
        "direct_copy_from_tdr": Field(
            bool, True, False, "Attempts to copy files directly from TDR; "
                               "if False, will copy to the staging area bucket first"
        )
    }
)
def ingest_data_files(context: AbstractComputeExecutionContext, data_entities: set[DataFileEntity]) -> None:
    """
    Ingests data files for the supplied set of DataEntities
    :param context:
    :param data_entities:
    :return:
    """
    storage_client = context.resources.gcs
    data_repo_client = context.resources.data_repo_client
    scratch_config: ScratchConfig = context.resources.scratch_config
    target_hca_dataset: TdrDataset = context.resources.target_hca_dataset
    load_tag = context.resources.load_tag
    direct_copy = context.solid_config["direct_copy_from_tdr"]

    control_file_path = _generate_control_file(context, data_entities, scratch_config, storage_client, direct_copy)
    _bulk_ingest_to_tdr(
        context,
        control_file_path,
        data_repo_client,
        scratch_config,
        target_hca_dataset,
        load_tag)


def _bulk_ingest_to_tdr(context: AbstractComputeExecutionContext,
                        control_file_path: str,
                        data_repo_client: RepositoryApi,
                        scratch_config: ScratchConfig,
                        target_hca_dataset: TdrDataset,
                        load_tag: str) -> None:
    payload = {
        "profileId": target_hca_dataset.billing_profile_id,
        "loadControlFile": f"gs://{scratch_config.scratch_bucket_name}/{control_file_path}",
        "loadTag": load_tag,
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

    result = data_repo_client.retrieve_job_result(id=job_id)
    if result['failedFiles'] > 0:
        raise DataFileIngestionFailure(
            f"File ingestion failed; job_id = {job_id} had failedFiles = {result['failedFiles']})")


def _generate_control_file(
    context: AbstractComputeExecutionContext,
        data_entities: set[DataFileEntity],
        scratch_config: ScratchConfig,
        storage_client: Client,
        direct_copy: bool
) -> str:
    ingest_items = []
    context.log.info("Copying files to staging bucket...")
    for data_entity in data_entities:
        # for extremely large datasets, we implement this workaround to directly copy out of TDR
        # this is not desirable as it requires a custom permission setting per source TDR dataset
        # and a manual intervention from a Jade team member to setup
        if direct_copy:
            ingest_items.append(
                f'{{"sourcePath":"{data_entity.access_url}", "targetPath":"{data_entity.target_path}"}}')
        else:
            file_bucket_and_prefix = parse_gs_path(data_entity.access_url)
            bucket = Bucket(storage_client, file_bucket_and_prefix.bucket)
            dest_bucket = Bucket(storage_client, scratch_config.scratch_bucket_name)

            blob: Blob = Blob(file_bucket_and_prefix.prefix, bucket)
            file_name = "/".join(blob.name.split("/")[1:])

            context.log.debug(
                f"Copying from {blob.name} to gs://{dest_bucket.name} / {scratch_config.scratch_prefix_name}/data_files/{file_name}")

            new_blob = bucket.copy_blob(
                blob, dest_bucket, f"{scratch_config.scratch_prefix_name}/data_files/{file_name}")
            ingest_items.append(
                f'{{"sourcePath":"gs://{dest_bucket.name}/{new_blob.name}", "targetPath":"{data_entity.target_path}"}}')

    # write out a JSONL control file for TDR to consume
    control_file_str = "\n".join(ingest_items)
    bucket = storage_client.get_bucket(scratch_config.scratch_bucket_name)
    control_file_path = f"{scratch_config.scratch_prefix_name}/data_ingest_requests/control_file.txt"
    control_file_upload = bucket.blob(
        control_file_path
    )

    context.log.info(f"Uploading control file to gs://{scratch_config.scratch_bucket_name}/{control_file_path}")
    control_file_upload.upload_from_string(client=storage_client, data=control_file_str)
    return control_file_path
