from google.cloud import bigquery
from google.cloud.bigquery import ExternalConfig, WriteDisposition


def build_query_job_using_external_schema(
        query: str,
        source_paths: list[str],
        schema: list[dict[str, str]],
        table_name: str,
        destination: str,
        bigquery_client: bigquery.client.Client,
        bigquery_project: str
) -> bigquery.QueryJob:
    """
    Performs a bigquery query using an external table definition, using the supplied
    schema and source paths. Results are deposited at the supplied GCS destination
    """
    job_config = bigquery.QueryJobConfig()

    external_config = {
        'sourceFormat': 'NEWLINE_DELIMITED_JSON',
        'sourceUris': source_paths,
        'schema': {'fields': schema}
    }
    external_config = ExternalConfig(external_config)
    job_config.table_definitions = {
        table_name: external_config
    }

    job_config.destination = destination
    job_config.use_legacy_sql = False
    job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE

    query_job = bigquery_client.query(
        query,
        job_config,
        location='US',
        project=bigquery_project
    )

    return query_job


def build_extract_job(
        source_table: str,
        out_path: str,
        bigquery_project: str,
        bigquery_client: bigquery.client.Client
) -> bigquery.ExtractJob:
    """
    Extracts the contents of a BQ table to the supplied out path
    """
    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
    extract_job: bigquery.ExtractJob = bigquery_client.extract_table(
        source_table,
        destination_uris=[
            f"{out_path}"
        ],
        job_config=job_config,
        location="US",
        project=bigquery_project
    )

    return extract_job
