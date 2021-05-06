from typing import Iterator

from dagster import composite_solid, solid, Nothing, InputDefinition
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster.experimental import DynamicOutput, DynamicOutputDefinition
from data_repo_client import JobModel
from google.cloud import bigquery
from google.cloud.bigquery import ExternalConfig, ExternalSourceFormat, SchemaField, WriteDisposition
from google.cloud.bigquery.client import RowIterator
from hca_manage.manage import JobId
from hca_orchestration.contrib.data_repo import retry_api_request
from hca_orchestration.resources.config.hca_dataset import TargetHcaDataset
from hca_orchestration.resources.config.scratch import ScratchConfig
from hca_orchestration.solids.data_repo import wait_for_job_completion
from hca_orchestration.support.typing import HcaScratchDatasetName


@solid(
    required_resource_keys={"bigquery_client", "scratch_config", "storage_client", "target_hca_dataset"},
    input_defs=[InputDefinition("scratch_dataset_name", HcaScratchDatasetName)],
    output_defs=[DynamicOutputDefinition(name="control_file_path", dagster_type=str)]
)
def diff_file_loads(context: AbstractComputeExecutionContext,
                    scratch_dataset_name: HcaScratchDatasetName) -> Iterator[str]:
    """
    Determine files to load by joining against the target HCA dataset
    then drop the result in our scratch bucket as a "control file"
    that Jade will use for bulk file ingest
    :param context: Dagster solid context
    :param scratch_dataset_name: Name of the "scratch" dataset we are using for this pipeline run
    :return:
    """
    target_hca_dataset = context.resources.target_hca_dataset
    file_load_table_name = 'file_load_requests'
    bigquery_client = context.resources.bigquery_client
    scratch = context.resources.scratch_config

    determine_files_to_load(
        bigquery_client,
        target_hca_dataset,
        scratch_dataset_name,
        file_load_table_name,
        scratch
    )

    extract_files_to_load(
        bigquery_client,
        context.resources.scratch_config,
        scratch_dataset_name,
        file_load_table_name
    )

    prefix = f'{scratch.scratch_prefix_name}/data-transfer-requests-deduped'
    context.log.debug(f"bucket = {scratch.scratch_bucket_name}; prefix={scratch.scratch_prefix_name}")
    storage_client = context.resources.storage_client
    for blob in storage_client.list_blobs(scratch.scratch_bucket_name, prefix=prefix):
        yield DynamicOutput(
            output_name='control_file_path',
            value=blob.name,
            mapping_key=blob.name.replace('/', '_').replace('-', '_')
        )


def determine_files_to_load(
        bigquery_client: bigquery.client.Client,
        target_hca_dataset: TargetHcaDataset,
        staging_dataset: HcaScratchDatasetName,
        file_load_table_name: str,
        scratch_config: ScratchConfig,
) -> RowIterator:
    fq_dataset_id = fully_qualified_jade_dataset_name(target_hca_dataset.dataset_name)
    query = f"""
    WITH J AS (
        SELECT target_path FROM `{target_hca_dataset.project_id}.{fq_dataset_id}.datarepo_load_history` WHERE state = 'succeeded'
    )
    SELECT S.source_path AS sourcePath, S.target_path as targetPath
    FROM {file_load_table_name} S LEFT JOIN J USING (target_path)
    WHERE J.target_path IS NULL
    """

    # configure the external table definition
    job_config = bigquery.QueryJobConfig()
    external_config: ExternalConfig = ExternalConfig(
        ExternalSourceFormat.NEWLINE_DELIMITED_JSON
    )
    external_config.source_uris = [
        f"{gs_path_from_bucket_prefix(scratch_config.scratch_bucket_name, scratch_config.scratch_prefix_name)}/data-transfer-requests/*"
    ]
    external_config.schema = [
        SchemaField("source_path", "STRING"),
        SchemaField("target_path", "STRING")
    ]
    job_config.table_definitions = {
        "file_load_requests": external_config
    }

    # setup the destination and other configs
    job_config.destination = f"{staging_dataset}.{file_load_table_name}"
    job_config.use_legacy_sql = False
    job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE  # TODO consider removing this or config'ing out

    # todo retry on 5xx failure
    query_job: bigquery.QueryJob = bigquery_client.query(
        query,
        job_config,
        location='US',
        project=scratch_config.scratch_bq_project
    )
    return query_job.result()


def extract_files_to_load(
        bigquery_client: bigquery.client.Client,
        scratch_config: ScratchConfig,
        scratch_dataset_name: HcaScratchDatasetName,
        file_load_table_name: str
) -> bigquery.ExtractJob:
    out_path = f"{gs_path_from_bucket_prefix(scratch_config.scratch_bucket_name, scratch_config.scratch_prefix_name)}/data-transfer-requests-deduped/*"

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
    extract_job: bigquery.ExtractJob = bigquery_client.extract_table(
        f"{scratch_dataset_name}.{file_load_table_name}",
        destination_uris=[
            f"{out_path}"
        ],
        job_config=job_config,
        location="US",
        project=scratch_config.scratch_bq_project
    )
    return extract_job.result()


@solid(
    required_resource_keys={"data_repo_client", "scratch_config", "load_tag", "target_hca_dataset"},
    input_defs=[InputDefinition("control_file_path", str)]
)
def run_bulk_file_ingest(context: AbstractComputeExecutionContext, control_file_path: str) -> str:
    # TODO bail out if the control file is empty
    profile_id = context.resources.target_hca_dataset.billing_profile_id
    dataset_id = context.resources.target_hca_dataset.dataset_id
    scratch_bucket_name = context.resources.scratch_config.scratch_bucket_name

    payload = {
        "profileId": profile_id,
        "loadControlFile": f"gs://{scratch_bucket_name}/{control_file_path}",
        "loadTag": context.resources.load_tag,
        "maxFailedFileLoads": 0
    }
    context.log.info(f'payload = {payload}')

    data_repo_client = context.resources.data_repo_client
    job_response: JobModel = data_repo_client.bulk_file_load(
        dataset_id,
        bulk_file_load=payload
    )
    context.log.info(f"bulk file ingest job id = {job_response.id}")
    result: JobId = JobId(job_response.id)

    return result


@solid(
    required_resource_keys={"data_repo_client"}
)
def check_bulk_file_ingest_job_result(context: AbstractComputeExecutionContext, job_id: str):
    retry_api_request(context.resources.data_repo_client.retrieve_job_result, 60, 2, {500}, job_id)


@composite_solid(
    input_defs=[InputDefinition("control_file_path", str)]
)
def bulk_ingest(control_file_path: str):
    job_id = run_bulk_file_ingest(control_file_path)
    wait_for_job_completion(job_id)
    check_bulk_file_ingest_job_result(job_id)


def gs_path_from_bucket_prefix(bucket: str, prefix: str) -> str:
    return f"gs://{bucket}/{prefix}"


def fully_qualified_jade_dataset_name(base_dataset_name: str) -> str:
    return f"datarepo_{base_dataset_name}"


@composite_solid(
    input_defs=[InputDefinition("scratch_dataset_name", HcaScratchDatasetName)]
)
def import_data_files(scratch_dataset_name: HcaScratchDatasetName) -> Nothing:
    generated_file_loads = diff_file_loads(scratch_dataset_name)
    bulk_ingest_jobs = generated_file_loads.map(bulk_ingest)
