from collections import defaultdict
from typing import DefaultDict

from dagster import solid, InputDefinition, Nothing
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)
from dagster_utils.contrib.data_repo.jobs import poll_job
from dagster_utils.contrib.data_repo.typing import JobId
from dagster_utils.contrib.google import GsBucketWithPrefix
from data_repo_client import JobModel, RepositoryApi
from google.cloud.storage.client import Client

from hca_orchestration.models.hca_dataset import HcaDataset
from hca_orchestration.models.scratch import ScratchConfig


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
    target_hca_dataset: HcaDataset = context.resources.target_hca_dataset

    entity_types = _find_entities_for_ingestion(gcs, scratch_config)
    ingest_tabular_data_to_tdr(context, data_repo_client, entity_types, target_hca_dataset)

    return set(entity_types.keys())


def ingest_tabular_data_to_tdr(context: AbstractComputeExecutionContext, data_repo_client: RepositoryApi,
                               entity_types: dict[str, GsBucketWithPrefix], target_hca_dataset: HcaDataset) -> None:
    for entity_type, path in entity_types.items():
        ingest_path = f"{path.to_gs_path()}/*"
        payload = {
            "format": "json",
            "ignore_unknown_values": "false",
            "max_bad_records": 0,
            "path": ingest_path,
            "table": entity_type
        }

        context.log.info(f"Submitting tabular data ingests for {entity_type}...")
        job_response: JobModel = data_repo_client.ingest_dataset(
            id=target_hca_dataset.dataset_id,
            ingest=payload
        )

        job_id = JobId(job_response.id)
        context.log.info(f"Job id = {job_id}")
        poll_job(job_id, 300, 2, data_repo_client)


def _find_entities_for_ingestion(gcs: Client,
                                 scratch_config: ScratchConfig) -> dict[str, GsBucketWithPrefix]:
    result = gcs.list_blobs(
        bucket_or_name=scratch_config.scratch_bucket_name,
        prefix=scratch_config.scratch_prefix_name + "/tabular_data_for_ingest/"
    )
    entity_types = {}

    for blob in result:
        entity_type = blob.name.split('/')[-2]
        last_idx = blob.name.rfind("/")
        entity_types[entity_type] = GsBucketWithPrefix(scratch_config.scratch_bucket_name, blob.name[0:last_idx])
    return entity_types
