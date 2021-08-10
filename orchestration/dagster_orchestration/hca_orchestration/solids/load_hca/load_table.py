import logging
from typing import Optional

from dagster import solid
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster_utils.contrib.data_repo.jobs import poll_job
from dagster_utils.contrib.data_repo.typing import JobId
from dagster_utils.contrib.google import parse_gs_path
from data_repo_client import RepositoryApi, JobModel
from google.cloud.bigquery import DestinationFormat
from google.cloud.bigquery.client import RowIterator
from google.cloud.storage import Client

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.contrib.gcs import path_has_any_data
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.models.scratch import ScratchConfig
from hca_orchestration.support.typing import HcaScratchDatasetName, MetadataType, MetadataTypeFanoutResult


def _diff_hca_table(
        metadata_type: MetadataType,
        metadata_path: str,
        primary_key: str,
        joined_table_name: str,
        scratch_config: ScratchConfig,
        target_hca_dataset: TdrDataset,
        scratch_dataset_name: HcaScratchDatasetName,
        bigquery_service: BigQueryService

) -> None:
    datarepo_key = f"{primary_key} as datarepo_{primary_key}, version as datarepo_version"
    fq_dataset_id = target_hca_dataset.fully_qualified_jade_dataset_name()

    query = f"""
    SELECT existing.datarepo_row_id, staged.*, {datarepo_key}
    FROM {metadata_type} staged FULL JOIN `{target_hca_dataset.project_id}.{fq_dataset_id}.{metadata_type}` existing
    USING ({primary_key}, version)
    """
    destination = f"{scratch_dataset_name}.{joined_table_name}"

    source_paths = [
        f"{scratch_config.scratch_area()}/{metadata_path}/{metadata_type}/*"
    ]

    bigquery_service.run_query_using_external_schema(
        query,
        schema=None,
        source_paths=source_paths,
        table_name=metadata_type,
        destination=destination,
        bigquery_project=scratch_config.scratch_bq_project
    )


def _query_rows_to_append(
        metadata_type: str,
        primary_key: str,
        scratch_config: ScratchConfig,
        scratch_dataset_name: HcaScratchDatasetName,
        joined_table_name: str,
        bigquery_service: BigQueryService
) -> RowIterator:
    query = f"""
    SELECT * EXCEPT (datarepo_{primary_key}, datarepo_row_id, datarepo_version)
    FROM {scratch_dataset_name}.{joined_table_name}
    WHERE datarepo_row_id IS NULL AND {primary_key} IS NOT NULL AND version IS NOT NULL
    """

    target_table = f"{scratch_dataset_name}.{metadata_type}_values"
    return bigquery_service.run_query_with_destination(
        query,
        target_table,
        scratch_config.scratch_bq_project
    )


def export_data(
        operation_name: str,
        table_name_extension: str,
        metadata_type: MetadataType,
        scratch_config: ScratchConfig,
        scratch_dataset_name: HcaScratchDatasetName,
        bigquery_service: BigQueryService
) -> int:
    assert table_name_extension.startswith("_"), "Export data extension must start with _"

    source_table_name = f"{metadata_type}{table_name_extension}"
    out_path = f"{scratch_config.scratch_area()}/{operation_name}/{metadata_type}/*"

    logging.info(f"Exporting data to {out_path}")
    num_rows = bigquery_service.get_num_rows_in_table(
        source_table_name,
        scratch_dataset_name
    )
    if num_rows == 0:
        return num_rows

    bigquery_service.build_extract_job(
        source_table=source_table_name,
        out_path=out_path,
        bigquery_dataset=scratch_dataset_name,
        bigquery_project=scratch_config.scratch_bq_project,
    )

    return num_rows


def _ingest_table(
        data_repo_api_client: RepositoryApi,
        target_dataset: TdrDataset,
        table_name: str,
        scratch_config: ScratchConfig
) -> JobId:
    source_path = f"{scratch_config.scratch_area()}/new-rows/{table_name}/*"

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


def start_load(
        scratch_config: ScratchConfig,
        scratch_dataset_name: HcaScratchDatasetName,
        target_hca_dataset: TdrDataset,
        metadata_type: MetadataType,
        metadata_path: str,
        data_repo_client: RepositoryApi,
        bigquery_service: BigQueryService
) -> Optional[JobId]:
    pk = f"{metadata_type}_id"
    joined_table_name = f"{metadata_type}_joined"
    _diff_hca_table(
        metadata_type=metadata_type,
        metadata_path=metadata_path,
        primary_key=pk,
        joined_table_name=joined_table_name,
        scratch_config=scratch_config,
        target_hca_dataset=target_hca_dataset,
        scratch_dataset_name=scratch_dataset_name,
        bigquery_service=bigquery_service,

    )
    _query_rows_to_append(
        metadata_type=metadata_type,
        primary_key=pk,
        scratch_config=scratch_config,
        scratch_dataset_name=scratch_dataset_name,
        joined_table_name=joined_table_name,
        bigquery_service=bigquery_service
    )

    num_new_rows = export_data(
        operation_name="new-rows",
        table_name_extension="_values",
        metadata_type=metadata_type,
        scratch_config=scratch_config,
        scratch_dataset_name=scratch_dataset_name,
        bigquery_service=bigquery_service
    )

    if num_new_rows > 0:
        job_id = _ingest_table(
            data_repo_client,
            target_hca_dataset,
            metadata_type,
            scratch_config
        )
        return poll_job(job_id, 600, 2, data_repo_client)
    return None


def _get_outdated_ids(
        table_name: str,
        target_hca_dataset: TdrDataset,
        scratch_config: ScratchConfig,
        bigquery_service: BigQueryService
) -> str:
    fq_dataset_id = target_hca_dataset.fully_qualified_jade_dataset_name()
    jade_table = f"{target_hca_dataset.project_id}.{fq_dataset_id}.{table_name}"
    out_path = f"{scratch_config.scratch_area()}/outdated-ids/{table_name}"

    query = f"""
    EXPORT DATA OPTIONS(
        uri='{out_path}/*',
        format='CSV',
        overwrite=true
    ) AS
    WITH latest_versions AS (
        SELECT {table_name}_id, MAX(version) AS latest_version
        FROM `{jade_table}` GROUP BY {table_name}_id
    )
    SELECT J.datarepo_row_id FROM
        `{jade_table}` J JOIN latest_versions L
        ON J.{table_name}_id = L.{table_name}_id
    WHERE J.version < L.latest_version
    """

    bigquery_service.run_query(
        query,
        bigquery_project=scratch_config.scratch_bq_project
    )

    return out_path


def clear_outdated(
        scratch_config: ScratchConfig,
        target_hca_dataset: TdrDataset,
        metadata_type: MetadataType,
        bigquery_service: BigQueryService,
        data_repo_client: RepositoryApi,
        gcs_client: Client
) -> Optional[JobId]:
    """
    Looks for any outdated IDs and submits a soft delete job to remove them from the target dataset

    :return: The JobID if any rows were found for deletion, or None if none were found
    """
    out_path = _get_outdated_ids(
        table_name=metadata_type,
        target_hca_dataset=target_hca_dataset,
        scratch_config=scratch_config,
        bigquery_service=bigquery_service
    )

    gs_path = parse_gs_path(out_path)
    if path_has_any_data(gs_path.bucket, gs_path.prefix, gcs_client):
        logging.info(f"Submitting soft deletes for path = {out_path}")
        outdated_ids_path = f"{scratch_config.scratch_area()}/outdated-ids/{metadata_type}/*"

        job_id: JobId = _soft_delete_outdated(
            data_repo_api_client=data_repo_client,
            target_dataset=target_hca_dataset,
            table_name=metadata_type,
            outdated_ids_path=outdated_ids_path
        )
        logging.info(f"Polling on job_id = {job_id}")
        return poll_job(job_id, 300, 2, data_repo_client)

    return None


def _soft_delete_outdated(
        data_repo_api_client: RepositoryApi,
        target_dataset: TdrDataset,
        table_name: str,
        outdated_ids_path: str
) -> JobId:
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


@solid(
    required_resource_keys={"bigquery_service", "target_hca_dataset", "scratch_config", "data_repo_client", "gcs"},
)
def load_table_solid(context: AbstractComputeExecutionContext,
                     metadata_fanout_result: MetadataTypeFanoutResult) -> Optional[JobId]:
    return load_table(
        context.resources.scratch_config,
        metadata_fanout_result.scratch_dataset_name,
        metadata_fanout_result.metadata_type,
        metadata_fanout_result.path,
        context.resources.target_hca_dataset,
        context.resources.gcs,
        context.resources.data_repo_client,
        context.resources.bigquery_service
    )


def load_table(
        scratch_config: ScratchConfig,
        scratch_dataset_name: HcaScratchDatasetName,
        metadata_type: MetadataType,
        metadata_path: str,
        target_hca_dataset: TdrDataset,
        gcs_client: Client,
        data_repo_client: RepositoryApi,
        bigquery_service: BigQueryService
) -> Optional[JobId]:
    source_path = f"{scratch_config.scratch_prefix_name}/{metadata_path}/{metadata_type}/"
    if not path_has_any_data(scratch_config.scratch_bucket_name, source_path, gcs_client):
        logging.info(f"No data for metadata type {metadata_type}")
        return None

    start_load(
        scratch_config,
        scratch_dataset_name,
        target_hca_dataset,
        metadata_type,
        metadata_path,
        data_repo_client,
        bigquery_service
    )

    maybe_outdated_job_id: Optional[JobId] = clear_outdated(
        scratch_config,
        target_hca_dataset,
        metadata_type,
        bigquery_service,
        data_repo_client,
        gcs_client
    )
    return maybe_outdated_job_id
