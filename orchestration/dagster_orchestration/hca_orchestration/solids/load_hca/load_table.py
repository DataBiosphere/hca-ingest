from dagster import composite_solid, solid, Nothing
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from data_repo_client import RepositoryApi, JobModel
from google.cloud.bigquery import Client, DestinationFormat
from google.cloud.bigquery.client import RowIterator

from hca_manage.common import JobId
from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.resources.config.hca_dataset import TargetHcaDataset
from hca_orchestration.resources.config.scratch import ScratchConfig
from hca_orchestration.solids.data_repo import wait_for_job_completion
from hca_orchestration.solids.load_hca.data_files.typing import FileMetadataType, FileMetadataTypeFanoutResult
from hca_orchestration.solids.load_hca.poll_ingest_job import check_data_ingest_job_result
from hca_orchestration.support.typing import HcaScratchDatasetName


def _diff_hca_table(
        file_metadata_type: FileMetadataType,
        primary_key: str,
        joined_table_name: str,
        scratch_config: ScratchConfig,
        target_hca_dataset: TargetHcaDataset,
        scratch_dataset_name: HcaScratchDatasetName,
        bigquery_service: BigQueryService

) -> None:
    datarepo_key = f"{primary_key} as datarepo_{primary_key}"
    fq_dataset_id = target_hca_dataset.fully_qualified_jade_dataset_name()

    query = f"""
    SELECT J.datarepo_row_id, S.*, {datarepo_key}
    FROM {file_metadata_type} S FULL JOIN {target_hca_dataset.project_id}.{fq_dataset_id}.{file_metadata_type} J
    USING ({primary_key})
    """
    destination = f"{scratch_dataset_name}.{joined_table_name}"
    source_paths = [
        f"gs://{scratch_config.scratch_bucket_name}/{scratch_config.scratch_prefix_name}/file-metadata-with-ids/{file_metadata_type}/*"]
    bigquery_service.build_query_job_using_external_schema(
        query,
        schema=None,
        source_paths=source_paths,
        table_name=file_metadata_type,
        destination=destination,
        bigquery_project=scratch_config.scratch_bq_project
    ).result()


def _query_rows_to_append(
        file_metadata_type: str,
        primary_key: str,
        scratch_config: ScratchConfig,
        scratch_dataset_name: HcaScratchDatasetName,
        joined_table_name: str,
        bigquery_service: BigQueryService
) -> RowIterator:
    query = f"""
    SELECT * EXCEPT (datarepo_{primary_key}, datarepo_row_id)
    FROM {scratch_dataset_name}.{joined_table_name}
    WHERE datarepo_row_id IS NULL AND {primary_key} IS NOT NULL
    """

    target_table = f"{scratch_dataset_name}.{file_metadata_type}_values"
    return bigquery_service.build_query_job(
        query,
        target_table,
        scratch_config.scratch_bq_project
    ).result()


def export_data(
        operation_name: str,
        table_name_extension: str,
        file_metadata_type: FileMetadataType,
        scratch_config: ScratchConfig,
        scratch_dataset_name: HcaScratchDatasetName,
        bigquery_service: BigQueryService
) -> None:
    assert table_name_extension.startswith("_"), "Export data extension must start with _"

    source_table_name = f"{file_metadata_type}{table_name_extension}"
    out_path = f"{scratch_config.scratch_area()}/{operation_name}/{file_metadata_type}/*"

    num_rows = bigquery_service.get_num_rows_in_table(
        source_table_name,
        scratch_dataset_name
    )
    if num_rows == 0:
        return

    bigquery_service.build_extract_job(
        source_table=source_table_name,
        out_path=out_path,
        bigquery_dataset=scratch_dataset_name,
        bigquery_project=scratch_config.scratch_bq_project,
    ).result()


def _ingest_table(
        data_repo_api_client: RepositoryApi,
        target_dataset: TargetHcaDataset,
        table_name: str,
        scratch_config: ScratchConfig
) -> JobId:
    source_path = f"gs://{scratch_config.scratch_area()}/new-rows/{table_name}/*"

    payload = {
        "format": "json",
        "ignore_unknown_values": "false",
        "max_bad_records": 0,
        "path": source_path,
        "table": table_name
    }
    job_response: JobModel = data_repo_api_client.ingest_dataset(
        id=target_dataset.dataset_id,
        ingest=payload
    )

    # todo error handling
    return JobId(job_response.id)


@solid(
    required_resource_keys={"bigquery_service", "target_hca_dataset", "scratch_config", "data_repo_client"}
)
def start_load(
        context: AbstractComputeExecutionContext,
        file_metadata_fanout_result: FileMetadataTypeFanoutResult
) -> JobId:
    bigquery_service = context.resources.bigquery_service
    target_hca_dataset = context.resources.target_hca_dataset
    scratch_config = context.resources.scratch_config
    file_metadata_type = file_metadata_fanout_result.file_metadata_type
    scratch_dataset_name = file_metadata_fanout_result.scratch_dataset_name
    data_repo_client = context.resources.data_repo_client

    pk = f"{file_metadata_type}_id"
    joined_table_name = f"{file_metadata_type}_joined"
    _diff_hca_table(
        file_metadata_type=file_metadata_type,
        primary_key=pk,
        joined_table_name=joined_table_name,
        scratch_config=scratch_config,
        target_hca_dataset=target_hca_dataset,
        scratch_dataset_name=scratch_dataset_name,
        bigquery_service=bigquery_service,

    )
    _query_rows_to_append(
        file_metadata_type=file_metadata_type,
        primary_key=pk,
        scratch_config=scratch_config,
        scratch_dataset_name=scratch_dataset_name,
        joined_table_name=joined_table_name,
        bigquery_service=bigquery_service
    )

    export_data(
        operation_name="new-rows",
        table_name_extension="_values",
        file_metadata_type=file_metadata_type,
        scratch_config=scratch_config,
        scratch_dataset_name=scratch_dataset_name,
        bigquery_service=bigquery_service
    )

    # todo do not attempt to ingest if there are no rows
    job_id = _ingest_table(
        data_repo_client,
        target_hca_dataset,
        file_metadata_type,
        scratch_config
    )

    return job_id


def _get_outdated_ids(
        table_name: str,
        target_hca_dataset: TargetHcaDataset,
        scratch_config: ScratchConfig,
        scratch_dataset_name: HcaScratchDatasetName,
        outdated_ids_table_name: str,
        bigquery_service: BigQueryService
) -> None:
    fq_dataset_id = target_hca_dataset.fully_qualified_jade_dataset_name()
    jade_table = f"{target_hca_dataset.project_id}.{fq_dataset_id}.{table_name}"
    destination = f"{scratch_dataset_name}.{outdated_ids_table_name}"

    query = f"""
    WITH latest_versions AS (
        SELECT {table_name}_id, MAX(version) AS latest_version
        FROM {jade_table} GROUP BY {table_name}_id
    )
    SELECT J.datarepo_row_id FROM
        {jade_table} J JOIN latest_versions L
        ON J.{table_name}_id = L.{table_name}_id
    WHERE J.version < L.latest_version
    """

    bigquery_service.build_query_job(
        query,
        destination,
        scratch_config.scratch_bq_project
    ).result()


def _export_outdated(
        file_metadata_type: FileMetadataType,
        outdated_ids_table_name: str,
        scratch_dataset_name: HcaScratchDatasetName,
        scratch_config: ScratchConfig,
        bigquery_service: BigQueryService
) -> None:
    out_path = f"{scratch_config.scratch_area()}/outdated-ids/{file_metadata_type}/*"
    bigquery_service.build_extract_job(
        source_table=outdated_ids_table_name,
        out_path=out_path,
        bigquery_dataset=scratch_dataset_name,
        bigquery_project=scratch_config.scratch_bq_project,
        output_format=DestinationFormat.CSV
    ).result()


@solid(
    required_resource_keys={"bigquery_service", "target_hca_dataset", "scratch_config", "data_repo_client"}
)
def clear_outdated(
        context: AbstractComputeExecutionContext,
        file_metadata_fanout_result: FileMetadataTypeFanoutResult
) -> JobId:
    file_metadata_type = file_metadata_fanout_result.file_metadata_type
    bigquery_service = context.resources.bigquery_service
    target_hca_dataset = context.resources.target_hca_dataset
    scratch_config = context.resources.scratch_config
    outdated_ids_table_name = f"{file_metadata_type}_outdated_ids"

    _get_outdated_ids(
        table_name=file_metadata_type,
        target_hca_dataset=target_hca_dataset,
        scratch_config=scratch_config,
        scratch_dataset_name=file_metadata_fanout_result.scratch_dataset_name,
        outdated_ids_table_name=outdated_ids_table_name,
        bigquery_service=bigquery_service
    )
    _export_outdated(
        file_metadata_type=file_metadata_type,
        outdated_ids_table_name=outdated_ids_table_name,
        scratch_dataset_name=file_metadata_fanout_result.scratch_dataset_name,
        scratch_config=scratch_config,
        bigquery_service=bigquery_service
    )
    job_id: JobId = _soft_delete_outdated(
        data_repo_api_client=context.resources.data_repo_client,
        target_dataset=target_hca_dataset,
        table_name=file_metadata_type,
        scratch_config=scratch_config
    )
    return job_id


def _soft_delete_outdated(
        data_repo_api_client: RepositoryApi,
        target_dataset: TargetHcaDataset,
        table_name: str,
        scratch_config: ScratchConfig
) -> JobId:
    # todo don't exec if outdated = 0
    outdated_ids_path = f"gs://{scratch_config.scratch_area()}/outdated-ids/{table_name}/*"
    payload = {
        "deleteType": "soft",
        "specType": "gcsFile",
        "tables": [
            {
                "gcsFileSpec": {
                    "fileType": "csv",
                    "path": outdated_ids_path
                },
                "tableName": table_name
            }
        ]
    }

    job_response: JobModel = data_repo_api_client.apply_dataset_data_deletion(
        id=target_dataset.dataset_id,
        data_deletion_request=payload
    )
    return JobId(job_response.id)


@composite_solid
def load_table(file_metadata_fanout_result: FileMetadataTypeFanoutResult) -> Nothing:
    """
    Composite solid that knows how to load a metadata table
    :param file_metadata_fanout_result:
    :return:
    """
    job_id = start_load(file_metadata_fanout_result)
    wait_for_job_completion(job_id)
    check_data_ingest_job_result(job_id)

    soft_delete_job_id = clear_outdated(file_metadata_fanout_result)
    wait_for_job_completion(soft_delete_job_id)
    check_data_ingest_job_result(soft_delete_job_id)
