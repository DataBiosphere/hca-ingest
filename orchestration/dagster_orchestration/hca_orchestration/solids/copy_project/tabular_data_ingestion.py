from collections import defaultdict

from dagster import solid, InputDefinition, Nothing
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)
from dagster_utils.contrib.data_repo.jobs import poll_job
from dagster_utils.contrib.data_repo.typing import JobId
from data_repo_client import JobModel, RepositoryApi
from google.cloud.storage.client import Client

from hca_orchestration.resources.config.hca_dataset import TargetHcaDataset
from hca_orchestration.resources.config.scratch import ScratchConfig


@solid(
    required_resource_keys={
        "gcs",
        "scratch_config",
        "data_repo_client",
        "target_hca_dataset"
    },
    input_defs=[InputDefinition("start", Nothing)]
)
def ingest_tabular_data(context: AbstractComputeExecutionContext) -> set[str]:
    gcs = context.resources.gcs
    scratch_config: ScratchConfig = context.resources.scratch_config
    data_repo_client = context.resources.data_repo_client
    target_hca_dataset: TargetHcaDataset = context.resources.target_hca_dataset

    entity_types = _find_entities_for_ingestion(context, gcs, scratch_config)
    _ingest_tabular_data_to_tdr(context, data_repo_client, entity_types, target_hca_dataset)

    return set(entity_types.keys())


def _ingest_tabular_data_to_tdr(context: AbstractComputeExecutionContext, data_repo_client: RepositoryApi,
                                entity_types: dict[str, str], target_hca_dataset: TargetHcaDataset) -> None:
    for entity_type, path in entity_types.items():
        payload = {
            "format": "json",
            "ignore_unknown_values": "false",
            "max_bad_records": 0,
            "path": path,
            "table": entity_type
        }

        context.log.info(f"Submitting request to TDR to ingest data at path = {path}")
        job_response: JobModel = data_repo_client.ingest_dataset(
            id=target_hca_dataset.dataset_id,
            ingest=payload
        )

        job_id = JobId(job_response.id)
        context.log.info(f"Job id = {job_id}")
        poll_job(job_id, 300, 2, data_repo_client)


def _find_entities_for_ingestion(context: AbstractComputeExecutionContext, gcs: Client,
                                 scratch_config: ScratchConfig) -> dict[str, str]:
    result = gcs.list_blobs(
        scratch_config.scratch_bucket_name,
        prefix=scratch_config.scratch_prefix_name +
        "/tabular_data_for_ingest")
    entity_types = defaultdict(str)
    for blob in result:
        blob.reload()
        if blob.size == 0:
            continue

        entity_type = blob.name.split('/')[-2]
        last_idx = blob.name.rfind("/")
        path = f"gs://{scratch_config.scratch_bucket_name}/{blob.name[0:last_idx]}/*"
        entity_types[entity_type] = path
        context.log.info(f"Found path {path} for ingest")
    return entity_types
