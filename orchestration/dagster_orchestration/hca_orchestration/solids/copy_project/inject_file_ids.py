from dagster import Nothing, InputDefinition, op
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)
from dagster_utils.contrib.google import GsBucketWithPrefix

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.models.scratch import ScratchConfig
from hca_orchestration.solids.copy_project.tabular_data_ingestion import ingest_tabular_data_to_tdr


@op(
    required_resource_keys={"data_repo_client", "bigquery_service", "scratch_config", "target_hca_dataset"},
    input_defs=[InputDefinition("start", Nothing)]
)
def inject_file_ids(context: AbstractComputeExecutionContext, entity_types: set[str]) -> set[str]:
    data_repo_client = context.resources.data_repo_client
    scratch_config: ScratchConfig = context.resources.scratch_config
    target_hca_dataset: TdrDataset = context.resources.target_hca_dataset
    bigquery_service: BigQueryService = context.resources.bigquery_service

    ingest_paths = {}
    for entity_type in entity_types:
        if not entity_type.endswith("_file"):
            continue

        destination_path = GsBucketWithPrefix(scratch_config.scratch_bucket_name,
                                              f"{scratch_config.scratch_prefix_name}/{entity_type}_with_ids")
        bigquery_service.run_extract_file_ids_job(
            destination_path,
            entity_type, target_hca_dataset, "us-central1"
        )
        ingest_paths[entity_type] = destination_path

    ingest_tabular_data_to_tdr(context, data_repo_client, ingest_paths, target_hca_dataset)
    return entity_types
