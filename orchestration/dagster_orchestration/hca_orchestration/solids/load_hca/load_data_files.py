from dagster import composite_solid, solid, Nothing, InputDefinition, DagsterLogManager, String
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

from hca_orchestration.support.typing import HcaStagingDatasetName, StagingBucket, StagingPrefix, JadeDatasetId

from google.cloud import bigquery
from google.cloud.bigquery.client import RowIterator
from google.cloud.bigquery import ExternalConfig, ExternalSourceFormat, SchemaField, WriteDisposition


@solid(
    required_resource_keys={"bigquery_client"},
    config_schema={
        'jade_project': String,
        'jade_dataset': JadeDatasetId,
        'staging_project': String,
        'staging_bucket': StagingBucket,
        'staging_prefix': StagingPrefix,
    },
    input_defs=[InputDefinition("staging_dataset_name", HcaStagingDatasetName)]
)
def diff_file_loads(context: AbstractComputeExecutionContext, staging_dataset_name: HcaStagingDatasetName):
    jade_project = context.solid_config["jade_project"]
    jade_dataset = context.solid_config["jade_dataset"]
    staging_project = context.solid_config["staging_project"]
    staging_bucket = context.solid_config["staging_bucket"]
    staging_prefix = context.solid_config["staging_prefix"]

    file_load_table_name = 'file_load_requests'
    bigquery_client = context.resources["bigquery_client"]
    determine_files_to_load(
        bigquery_client,
        staging_bucket,
        staging_prefix,
        jade_dataset,
        jade_project,
        staging_project,
        staging_dataset_name,
        file_load_table_name,
        context.log
    )


def determine_files_to_load(
        bigquery_client: bigquery.client.Client,
        staging_bucket: StagingBucket,
        staging_prefix: StagingPrefix,
        jade_dataset: JadeDatasetId,
        jade_project: str,
        staging_project: str,
        staging_dataset: HcaStagingDatasetName,
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


def gs_path_from_bucket_prefix(bucket: StagingBucket, prefix: StagingPrefix):
    return f"gs://{bucket}/{prefix}"


@composite_solid(
    input_defs=[InputDefinition("staging_dataset_name", HcaStagingDatasetName)]
)
def import_data_files(staging_dataset_name: HcaStagingDatasetName) -> Nothing:
    # TODO this is a stub for this branch of the pipeline
    diff_file_loads(staging_dataset_name)
