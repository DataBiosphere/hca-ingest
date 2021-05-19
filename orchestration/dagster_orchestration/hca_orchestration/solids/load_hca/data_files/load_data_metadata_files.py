from enum import Enum
from typing import Iterator

from dagster import solid, composite_solid, Nothing
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster.experimental import DynamicOutput, DynamicOutputDefinition
from google.cloud.bigquery.client import RowIterator

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.resources.config.hca_dataset import TargetHcaDataset
from hca_orchestration.resources.config.scratch import ScratchConfig
from hca_orchestration.solids.load_hca.data_files.typing import FileMetadataType, \
    FileMetadataTypeFanoutResult
from hca_orchestration.solids.load_hca.load_table import load_table, export_data
from hca_orchestration.support.typing import HcaScratchDatasetName


class FileMetadataTypes(Enum):
    ANALYSIS_FILE = FileMetadataType('analysis_file')
    IMAGE_FILE = FileMetadataType('image_file')
    REFERENCE_FILE = FileMetadataType('reference_file')
    SEQUENCE_FILE = FileMetadataType('sequence_file')
    SUPPLEMENTARY_FILE = FileMetadataType('supplementary_file')


@solid(
    output_defs=[
        DynamicOutputDefinition(name="table_fanout_result", dagster_type=FileMetadataTypeFanoutResult)
    ]
)
def ingest_metadata_type(scratch_dataset_name: HcaScratchDatasetName) -> Iterator[FileMetadataTypeFanoutResult]:
    """
    For each file type, return a dynamic output over which we can later map
    This saves us from hardcoding solids for each file type
    """
    for file_metadata_type in FileMetadataTypes:
        yield DynamicOutput(
            value=FileMetadataTypeFanoutResult(scratch_dataset_name, file_metadata_type.value),
            mapping_key=file_metadata_type.value,
            output_name="table_fanout_result"
        )


def _inject_file_ids(
        target_hca_dataset: TargetHcaDataset,
        scratch_config: ScratchConfig,
        file_metadata_type: str,
        scratch_dataset_name: HcaScratchDatasetName,
        bigquery_service: BigQueryService,
) -> RowIterator:
    fq_dataset_id = target_hca_dataset.fully_qualified_jade_dataset_name()

    query = f"""
    SELECT S.{file_metadata_type}_id, S.version, J.file_id, S.content, S.descriptor
    FROM {file_metadata_type} S LEFT JOIN `{target_hca_dataset.project_id}.{fq_dataset_id}.datarepo_load_history` J
    ON J.state = 'succeeded'
    AND JSON_EXTRACT_SCALAR(S.descriptor, '$.crc32c') = J.checksum_crc32c
    AND '/' || JSON_EXTRACT_SCALAR(S.descriptor, '$.file_id') || '/' || JSON_EXTRACT_SCALAR(S.descriptor, '$.file_name') = J.target_path
    """

    destination_table_name = f"{file_metadata_type}_with_ids"
    source_path = f"{scratch_config.scratch_area()}/metadata/{file_metadata_type}/*"
    query_job = bigquery_service.build_query_job_using_external_schema(
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
        bigquery_project=scratch_config.scratch_bq_project
    ).result()

    return query_job


@solid(
    required_resource_keys={"bigquery_service", "target_hca_dataset", "scratch_config", "data_repo_client"}
)
def ingest_metadata_for_file_type(
        context: AbstractComputeExecutionContext,
        file_metadata_fanout_result: FileMetadataTypeFanoutResult
) -> None:
    bigquery_service = context.resources.bigquery_service
    target_hca_dataset = context.resources.target_hca_dataset
    scratch_config = context.resources.scratch_config
    file_metadata_type = file_metadata_fanout_result.file_metadata_type
    scratch_dataset_name = file_metadata_fanout_result.scratch_dataset_name

    _inject_file_ids(
        target_hca_dataset=target_hca_dataset,
        scratch_config=scratch_config,
        file_metadata_type=file_metadata_fanout_result.file_metadata_type,
        scratch_dataset_name=file_metadata_fanout_result.scratch_dataset_name,
        bigquery_service=bigquery_service,
    )
    export_data(
        "file-metadata-with-ids",
        table_name_extension="_with_ids",
        file_metadata_type=file_metadata_type,
        scratch_config=scratch_config,
        scratch_dataset_name=scratch_dataset_name,
        bigquery_service=bigquery_service
    )


@composite_solid
def ingest_metadata(file_metadata_fanout_result: FileMetadataTypeFanoutResult) -> Nothing:
    ingest_metadata_for_file_type(file_metadata_fanout_result)
    load_table(file_metadata_fanout_result)


@composite_solid
def file_metadata_fanout(scratch_dataset_name: HcaScratchDatasetName) -> Nothing:
    ingest_metadata_type(scratch_dataset_name).map(ingest_metadata)
