from collections import defaultdict

from dagster import solid, Nothing, InputDefinition
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)
from hca_orchestration.models.hca_dataset import HcaDataset

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.models.scratch import ScratchConfig
from hca_orchestration.solids.copy_project.tabular_data_ingestion import ingest_tabular_data_to_tdr


@solid(
    required_resource_keys={"data_repo_client", "bigquery_service", "scratch_config", "target_hca_dataset"},
    input_defs=[InputDefinition("start", Nothing)]
)
def inject_file_ids(context: AbstractComputeExecutionContext, entity_types: set[str]) -> set[str]:
    data_repo_client = context.resources.data_repo_client
    scratch_config: ScratchConfig = context.resources.scratch_config
    target_hca_dataset: HcaDataset = context.resources.target_hca_dataset
    bigquery_service: BigQueryService = context.resources.bigquery_service

    ingest_paths = defaultdict(str)
    for entity_type in entity_types:
        if not entity_type.endswith("_file"):
            continue

        destination_path = f"gs://{scratch_config.scratch_bucket_name}/{scratch_config.scratch_prefix_name}/{entity_type}_with_ids/*"
        query = f"""
        EXPORT DATA OPTIONS(
            uri='{destination_path}',
            format='JSON',
            overwrite=true
        ) AS
        SELECT sf.{entity_type}_id, sf.version, dlh.file_id, sf.content, sf.descriptor FROM `{target_hca_dataset.project_id}.datarepo_{target_hca_dataset.dataset_name}.{entity_type}` sf
        LEFT JOIN  `{target_hca_dataset.project_id}.datarepo_{target_hca_dataset.dataset_name}.datarepo_load_history` dlh
            ON dlh.state = 'succeeded' AND JSON_EXTRACT_SCALAR(sf.descriptor, '$.crc32c') = dlh.checksum_crc32c
            AND '/' || JSON_EXTRACT_SCALAR(sf.descriptor, '$.file_id') || '/' || JSON_EXTRACT_SCALAR(sf.descriptor, '$.file_name') = dlh.target_path
        """
        context.log.info(f"Querying data file metadata with file ids for {entity_type}, storing at {destination_path}")
        bigquery_service.build_query_job(
            query,
            bigquery_project=target_hca_dataset.project_id,
            location='us-central1').result()

        ingest_paths[entity_type] = destination_path

    ingest_tabular_data_to_tdr(context, data_repo_client, ingest_paths, target_hca_dataset)

    return entity_types
