from typing import Optional

from google.cloud import bigquery
from google.cloud.bigquery import ExternalConfig, WriteDisposition, QueryJob


def build_query_job(
        query: str,
        destination_table: str,
        bigquery_client: bigquery.client.Client,
        bigquery_project: str
) -> QueryJob:
    """
    Performs a bigquery query, with no external table definitions.
    Results are deposited in the destination table provided.
    """
    job_config = bigquery.QueryJobConfig()

    job_config.destination = destination_table
    job_config.use_legacy_sql = False
    job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
    query_job = bigquery_client.query(
        query,
        job_config,
        location=US,
        project=bigquery_project
    )

    return query_job


def build_query_job_using_external_schema(
        query: str,
        source_paths: list[str],
        schema: Optional[list[dict[str, str]]],
        table_name: str,
        destination: str,
        bigquery_client: bigquery.client.Client,
        bigquery_project: str
) -> bigquery.QueryJob:
    """
    Performs a bigquery query using an external table definition, using the supplied
    schema and source paths. Results are deposited at the supplied GCS destination

    If no schema is provided, we leverage BigQuery's schema autodetection mechanism
    to infer datatypes (https://cloud.google.com/bigquery/docs/schema-detect)
    """
    job_config = bigquery.QueryJobConfig()

    raw_external_config: dict[str, object] = {
        'sourceFormat': 'NEWLINE_DELIMITED_JSON',
    }
    if schema:
        raw_external_config['schema'] = {'fields': schema}
    else:
        raw_external_config['autodetect'] = True

    raw_external_config['sourceUris'] = source_paths

    external_config = ExternalConfig.from_api_repr(raw_external_config)
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
        bigquery_dataset: str,
        bigquery_project: str,
        bigquery_client: bigquery.client.Client,
        output_format: bigquery.DestinationFormat = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
) -> bigquery.ExtractJob:
    """
    Extracts the contents of a BQ table to the supplied out path
    """
    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = output_format
    job_config.print_header = False
    extract_job: bigquery.ExtractJob = bigquery_client.extract_table(
        f"{bigquery_dataset}.{source_table}",
        destination_uris=[
            f"{out_path}"
        ],
        job_config=job_config,
        project=bigquery_project
    )

    return extract_job


def get_num_rows_in_table(
        table_name: str,
        bigquery_dataset: str,
        bigquery_client: bigquery.client.Client
) -> int:
    """
    Returns the # of rows present in the given table
    table_name should be bare, without dataset or project.
    """
    query = f"""
    SELECT COUNT(1) FROM {bigquery_dataset}.{table_name}
    """
    result = bigquery_client.query(query, None).result()
    cnt = -1
    for row in result:
        cnt = row[0]
    return cnt
