from typing import Iterator

from dagster import composite_solid, solid
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster.experimental import DynamicOutput, DynamicOutputDefinition
from data_repo_client import JobModel
from google.cloud import bigquery
from google.cloud.bigquery.client import RowIterator

from hca_manage.common import JobId
from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.models.hca_dataset import TdrDataset
from hca_orchestration.models.scratch import ScratchConfig
from hca_orchestration.solids.data_repo import wait_for_job_completion
from hca_orchestration.solids.load_hca.poll_ingest_job import check_data_ingest_job_result
from hca_orchestration.support.typing import HcaScratchDatasetName

FILE_LOAD_TABLE_BQ_SCHEMA = [
    {
        'mode': 'NULLABLE',
        'name': 'source_path',
        'type': 'STRING'
    },
    {
        'mode': 'NULLABLE',
        'name': 'target_path',
        'type': 'STRING',
    }
]

FILE_LOAD_TABLE_NAME = 'file_load_requests'


@solid(
    required_resource_keys={"bigquery_service", "scratch_config", "gcs", "target_hca_dataset"},
    output_defs=[DynamicOutputDefinition(name="control_file_path", dagster_type=str)]
)
def diff_file_loads(context: AbstractComputeExecutionContext,
                    scratch_dataset_name: HcaScratchDatasetName) -> Iterator[str]:
    """
    Determine files to load by joining against the target HCA dataset, then drop the result in our scratch bucket as a
     "control file" that Jade will use for bulk file ingest
    :param context: Dagster solid context
    :param scratch_dataset_name: Name of the "scratch" dataset we are using for this pipeline run
    :return: Yields a list of control file blob names
    """
    target_hca_dataset = context.resources.target_hca_dataset
    bigquery_service = context.resources.bigquery_service
    scratch = context.resources.scratch_config

    _determine_files_to_load(
        bigquery_service,
        target_hca_dataset,
        scratch_dataset_name,
        FILE_LOAD_TABLE_NAME,
        scratch
    )

    _extract_files_to_load_to_control_files(
        bigquery_service,
        context.resources.scratch_config,
        scratch_dataset_name,
        FILE_LOAD_TABLE_NAME
    )

    prefix = f'{scratch.scratch_prefix_name}/data-transfer-requests-deduped'
    storage_client = context.resources.gcs
    for blob in storage_client.list_blobs(scratch.scratch_bucket_name, prefix=prefix):
        yield DynamicOutput(
            output_name='control_file_path',
            value=blob.name,
            mapping_key=blob.name.replace('/', '_').replace('-', '_')  # filter out bad chars for dagster dynamic output
        )


def _determine_files_to_load(
        bigquery_service: BigQueryService,
        target_hca_dataset: TdrDataset,
        staging_dataset: HcaScratchDatasetName,
        file_load_table_name: str,
        scratch_config: ScratchConfig,
) -> RowIterator:
    """
    Joins against the load history in the given TDR dataset and outputs the files not present
    to the given file_load_table_name parameter in the scratch dataset
    """
    fq_dataset_id = target_hca_dataset.fully_qualified_jade_dataset_name()
    query = f"""
    WITH J AS (
        SELECT target_path FROM `{target_hca_dataset.project_id}.{fq_dataset_id}.datarepo_load_history` WHERE state = 'succeeded'
    )
    SELECT S.source_path AS sourcePath, S.target_path as targetPath
    FROM {file_load_table_name} S LEFT JOIN J USING (target_path)
    WHERE J.target_path IS NULL
    """

    source_paths = [
        f"{scratch_config.scratch_area()}/data-transfer-requests/*"
    ]

    rows = bigquery_service.run_query_using_external_schema(
        query,
        source_paths,
        FILE_LOAD_TABLE_BQ_SCHEMA,
        file_load_table_name,
        f"{staging_dataset}.{file_load_table_name}",
        scratch_config.scratch_bq_project
    )
    return rows


def _extract_files_to_load_to_control_files(
        bigquery_service: BigQueryService,
        scratch_config: ScratchConfig,
        scratch_dataset_name: HcaScratchDatasetName,
        file_load_table_name: str
) -> bigquery.ExtractJob:
    # dump the contents of our file load requests table to a file in GCS suitable for use as a
    # control file for jade bulk data ingest
    out_path = f"{scratch_config.scratch_area()}/data-transfer-requests-deduped/*"

    extract_job = bigquery_service.build_extract_job(
        file_load_table_name,
        out_path,
        scratch_dataset_name,
        scratch_config.scratch_bq_project,
    )
    return extract_job


@solid(
    required_resource_keys={"data_repo_client", "scratch_config", "load_tag", "target_hca_dataset"},
)
def run_bulk_file_ingest(context: AbstractComputeExecutionContext, control_file_path: str) -> JobId:
    """
    Submits the given control for ingestion to TDR
    :param context: Dagster solid context
    :param control_file_path: Path to the control file for ingest
    :return: Jade Job ID
    """
    profile_id = context.resources.target_hca_dataset.billing_profile_id
    dataset_id = context.resources.target_hca_dataset.dataset_id
    scratch_bucket_name = context.resources.scratch_config.scratch_bucket_name

    payload = {
        "profileId": profile_id,
        "loadControlFile": f"gs://{scratch_bucket_name}/{control_file_path}",
        "loadTag": context.resources.load_tag,
        "maxFailedFileLoads": 0
    }
    context.log.info(f'Bulk file ingest payload = {payload}')

    data_repo_client = context.resources.data_repo_client
    job_response: JobModel = data_repo_client.bulk_file_load(
        dataset_id,
        bulk_file_load=payload
    )
    context.log.info(f"bulk file ingest job id = {job_response.id}")
    return JobId(job_response.id)


@composite_solid
def bulk_ingest(control_file_path: str) -> JobId:
    """
    Composite solid that submits the given control file for ingest to TDR, then polls on the resulting job
    to completion.
    :param control_file_path: GS path to the ingest control file
    """
    job_id: JobId = run_bulk_file_ingest(control_file_path)
    wait_for_job_completion(job_id)
    check_data_ingest_job_result(job_id)
    return job_id


@composite_solid(
    output_defs=[DynamicOutputDefinition(name="result", dagster_type=JobId)]
)
def import_data_files(scratch_dataset_name: HcaScratchDatasetName) -> list[JobId]:
    """
    Composite solid responsible for ingesting data files and related descriptors to TDR
    :param scratch_dataset_name: Scratch dataset that will hold temporary ingest data
    """
    generated_file_loads = diff_file_loads(scratch_dataset_name)
    bulk_ingest_jobs: list[JobId] = generated_file_loads.map(bulk_ingest)
    return bulk_ingest_jobs
