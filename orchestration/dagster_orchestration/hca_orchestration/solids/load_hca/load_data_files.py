from dagster import composite_solid, solid, InputDefinition, Nothing, OutputDefinition, String, DagsterLogManager
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster.experimental import DynamicOutput, DynamicOutputDefinition
from google.cloud import bigquery
from google.cloud.bigquery import ExternalConfig, ExternalSourceFormat, SchemaField, WriteDisposition
from google.cloud.bigquery.table import RowIterator
from google.cloud.storage.blob import Blob

from data_repo_client import JobModel
from hca_orchestration.pipelines.utils import JadeDatasetId, HcaStagingDatasetName, gs_path_from_bucket_prefix, \
    StagingBucket, StagingPrefix, JobId


@solid(
    required_resource_keys={"storage_client", "bigquery_client"},
    config_schema={
        'jade_project': String,
        'jade_dataset': JadeDatasetId,
        'staging_project': String,
        'staging_bucket': StagingBucket,
        'staging_prefix': StagingPrefix,
    },
    input_defs=[InputDefinition("staging_dataset", HcaStagingDatasetName)],
    output_defs=[DynamicOutputDefinition(name="control_file", dagster_type=Blob)]
)
def diff_file_loads(context: AbstractComputeExecutionContext, staging_dataset: HcaStagingDatasetName) -> Blob:
    """
    Joins the target table in the given TDR dataset against an external table definition (our staged files in GS),
    looking for files in the staging area that have not been loaded
    """
    context.log.info(
        f"diff_file_loads; staging_dataset = {staging_dataset}"
    )
    jade_project = context.solid_config["jade_project"]
    jade_dataset = context.solid_config["jade_dataset"]
    staging_project = context.solid_config["staging_project"]
    staging_bucket = context.solid_config["staging_bucket"]
    staging_prefix = context.solid_config["staging_prefix"]

    # join against the existing TDR load history to determine what is new and needs to be loaded
    file_load_table_name = 'file_load_requests'
    determine_files_to_load(
        context.resources.bigquery_client,
        staging_bucket,
        staging_prefix,
        jade_dataset,
        jade_project,
        staging_project,
        staging_dataset,
        file_load_table_name,
        context.log
    )

    # dump the file loads to a file in GCS
    extract_files_to_load(
        context.resources.bigquery_client,
        staging_bucket,
        staging_prefix,
        staging_project,
        staging_dataset,
        file_load_table_name
    )

    prefix = f'{staging_prefix}/data-transfer-requests-deduped'
    context.log.debug(f"bucket = {staging_bucket}; prefix={prefix}")
    storage_client = context.resources.storage_client
    for blob in storage_client.list_blobs(staging_bucket, prefix=prefix):
        yield DynamicOutput(
            output_name='control_file',
            value=blob,
            mapping_key=blob.name.replace('/', '_').replace('-', '_')
        )


def extract_files_to_load(
        bigquery_client: bigquery.client.Client,
        staging_bucket: StagingBucket,
        staging_prefix: StagingPrefix,
        staging_project, staging_dataset: HcaStagingDatasetName,
        file_load_table_name: str
) -> RowIterator:
    out_path = f"{gs_path_from_bucket_prefix(staging_bucket, staging_prefix)}/data-transfer-requests-deduped/*"
    # TODO clear out any files at the location

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
    extract_job: bigquery.ExtractJob = bigquery_client.extract_table(
        f"{staging_project}.{staging_dataset}.{file_load_table_name}",
        destination_uris=[
            f"{out_path}"
        ],
        job_config=job_config,
        location="US",
        project=staging_project
    )
    return extract_job.result()


def determine_files_to_load(
        bigquery_client: bigquery.client.Client,
        staging_bucket: StagingBucket,
        staging_prefix: StagingPrefix,
        jade_dataset: JadeDatasetId,
        jade_project: str,
        staging_project: str,
        staging_dataset: str,
        file_load_table_name: str,
        logger: DagsterLogManager
) -> RowIterator:
    query = f"""
    WITH J AS (
        SELECT target_path FROM `{jade_project}.{jade_dataset}.datarepo_load_history` WHERE state = 'succeeded'
    )
    SELECT S.source_path AS sourcePath, S.target_path as targetPath
    FROM {file_load_table_name} S LEFT JOIN J USING (target_path)
    WHERE J.target_path IS NULL
    """

    # configured the external table definition
    logger.debug(query)
    job_config = bigquery.QueryJobConfig()
    external_config: ExternalConfig = ExternalConfig(
        ExternalSourceFormat.NEWLINE_DELIMITED_JSON
    )
    external_config.source_uris = [
        f"{gs_path_from_bucket_prefix(staging_bucket, staging_prefix)}/data-transfer-requests/*"
    ]
    external_config.schema = [
        SchemaField("source_path", "STRING"),
        SchemaField("target_path", "STRING")
    ]
    job_config.table_definitions = {
        "file_load_requests": external_config
    }

    # setup the destination and other configs
    job_config.destination = f"{staging_project}.{staging_dataset}.{file_load_table_name}"
    job_config.use_legacy_sql = False
    job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE  # TODO consider removing this or config'ing out

    query_job: bigquery.QueryJob = bigquery_client.query(
        query,
        job_config,
        location='US',
        project=staging_project
    )
    return query_job.result()


@solid(
    required_resource_keys={"data_repo_client"},
    config_schema={
        'jade_dataset': JadeDatasetId,
        'jade_profile_id': String,
        'load_tag': String,
        'staging_bucket': StagingBucket
    },
    input_defs=[InputDefinition("control_file", Blob)]
)
def run_bulk_file_ingest(context: AbstractComputeExecutionContext, control_file: Blob) -> JobId:
    load_tag = context.solid_config['load_tag']
    profile_id = context.solid_config['jade_profile_id']
    dataset_id = context.solid_config['jade_dataset']
    staging_bucket = context.solid_config["staging_bucket"]

    payload = {
        "profileId": profile_id,
        "loadControlFile": f"gs://{staging_bucket}/{control_file.name}",
        "loadTag": load_tag,
        "maxFailedFileLoads": 0
    }
    context.log.debug(f'payload = {payload}')

    data_repo_client = context.resources.data_repo_client
    job_response: JobModel = data_repo_client.bulk_file_load(
        dataset_id,
        bulk_file_load=payload
    )
    context.log.info(f"bulk file ingest job id = {job_response.id}")
    return job_response.id


@solid(
    input_defs=[InputDefinition("nothing", Nothing)],
    output_defs=[OutputDefinition(name="fake_result", dagster_type=Nothing)]
)
def ingest_file_metadata(_context) -> Nothing:
    """
    This solid will ingest the related descriptor files into TDR
    TODO
    """
    pass


@composite_solid(
    input_defs=[InputDefinition("staging_dataset_name", HcaStagingDatasetName)]
)
def import_data_files(staging_dataset_name: HcaStagingDatasetName) -> Nothing:
    generated_file_loads = diff_file_loads(staging_dataset_name)
    result = generated_file_loads.map(run_bulk_file_ingest)

    # TODO this is a placeholder, remove
    return ingest_file_metadata()

    # todo fan out after files are bulk loaded
    # (run_bulk_file_ingest(
    # list_deduped_requests(extract_file_loads(diff_file_loads(table_fanout_result)))))
