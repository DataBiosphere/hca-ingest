from enum import Enum

from dagster import solid, composite_solid, configured, DynamicOutputDefinition, Optional, Failure
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from google.cloud.bigquery.client import RowIterator

from hca_manage.common import JobId
from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.resources.config.scratch import ScratchConfig
from hca_orchestration.solids.load_hca.ingest_metadata_type import ingest_metadata_type
from hca_orchestration.solids.load_hca.load_table import load_table_solid, export_data
from hca_orchestration.support.typing import HcaScratchDatasetName, MetadataType, MetadataTypeFanoutResult


class FileMetadataTypes(Enum):
    """
    This Enum captures MetadataTypes that are directly describing a file type in the HCA
    """
    ANALYSIS_FILE = MetadataType('analysis_file')
    IMAGE_FILE = MetadataType('image_file')
    REFERENCE_FILE = MetadataType('reference_file')
    SEQUENCE_FILE = MetadataType('sequence_file')
    SUPPLEMENTARY_FILE = MetadataType('supplementary_file')


ingest_file_metadata_type = configured(ingest_metadata_type, name="ingest_file_metadata_type")(
    {"metadata_types": FileMetadataTypes, "prefix": "file-metadata-with-ids"})


class NullFileIdException(Failure):
    pass


def _inject_file_ids(
        target_hca_dataset: TdrDataset,
        scratch_config: ScratchConfig,
        file_metadata_type: str,
        scratch_dataset_name: HcaScratchDatasetName,
        bigquery_service: BigQueryService,
        context: AbstractComputeExecutionContext
) -> RowIterator:
    fq_dataset_id = target_hca_dataset.fully_qualified_jade_dataset_name()

    query = f"""
    SELECT S.{file_metadata_type}_id, S.version, J.file_id, S.content, S.descriptor
    FROM {file_metadata_type} S LEFT JOIN `{target_hca_dataset.project_id}.{fq_dataset_id}.datarepo_load_history` J
    ON J.state = 'succeeded'
    AND JSON_EXTRACT_SCALAR(S.descriptor, '$.crc32c') = J.checksum_crc32c
    AND '/v1/' || JSON_EXTRACT_SCALAR(S.descriptor, '$.file_id') || '/' || JSON_EXTRACT_SCALAR(S.descriptor, '$.crc32c') || '/' || JSON_EXTRACT_SCALAR(S.descriptor, '$.file_name') = J.target_path
    """

    destination_table_name = f"{file_metadata_type}_with_ids"
    source_path = f"{scratch_config.scratch_area()}/metadata/{file_metadata_type}/*"

    rows = bigquery_service.run_query_using_external_schema(
        query,
        source_paths=[source_path],
        schema=[
            {
                "mode": "REQUIRED",
                "name": f"{file_metadata_type}_id",
                "type": "STRING"
            },
            {
                "mode": "REQUIRED",
                "name": "version",
                "type": "TIMESTAMP"
            },
            {
                "mode": "REQUIRED",
                "name": "content",
                "type": "STRING"
            },
            {
                "mode": "REQUIRED",
                "name": "crc32c",
                "type": "STRING"
            },
            {
                "mode": "REQUIRED",
                "name": "descriptor",
                "type": "STRING"
            }
        ],
        table_name=file_metadata_type,
        destination=f"{scratch_dataset_name}.{destination_table_name}",
        bigquery_project=scratch_config.scratch_bq_project,
        location=target_hca_dataset.bq_location
    )
    for row in rows:
        if not row["file_id"]:
            raise NullFileIdException(
                f"File metadata with null file ID detected, check crc32c and target_path [table={file_metadata_type}]")
    return rows


@solid(
    required_resource_keys={"bigquery_service", "target_hca_dataset", "scratch_config", "data_repo_client"}
)
def inject_file_ids_solid(
        context: AbstractComputeExecutionContext,
        file_metadata_fanout_result: MetadataTypeFanoutResult
) -> MetadataTypeFanoutResult:
    bigquery_service = context.resources.bigquery_service
    target_hca_dataset = context.resources.target_hca_dataset
    scratch_config = context.resources.scratch_config
    file_metadata_type = file_metadata_fanout_result.metadata_type
    scratch_dataset_name = file_metadata_fanout_result.scratch_dataset_name

    _inject_file_ids(
        target_hca_dataset=target_hca_dataset,
        scratch_config=scratch_config,
        file_metadata_type=file_metadata_fanout_result.metadata_type,
        scratch_dataset_name=file_metadata_fanout_result.scratch_dataset_name,
        bigquery_service=bigquery_service,
        context=context
    )
    export_data(
        "file-metadata-with-ids",
        table_name_extension="_with_ids",
        metadata_type=file_metadata_type,
        scratch_config=scratch_config,
        scratch_dataset_name=scratch_dataset_name,
        bigquery_service=bigquery_service
    )

    return file_metadata_fanout_result


@composite_solid(
    output_defs=[DynamicOutputDefinition(dagster_type=Optional[JobId])]
)
def file_metadata_fanout(result: list[JobId], scratch_dataset_name: HcaScratchDatasetName) -> Optional[JobId]:
    results = ingest_file_metadata_type(result, scratch_dataset_name).map(inject_file_ids_solid)
    return results.map(load_table_solid)
