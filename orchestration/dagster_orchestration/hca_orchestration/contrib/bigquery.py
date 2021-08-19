"""
Abstraction over the raw bigquery client. All operations automatically return the materialized results of a query.
"""

from dataclasses import dataclass
from typing import Optional

from dagster_utils.contrib.google import GsBucketWithPrefix
from google.cloud import bigquery
from google.cloud.bigquery import ExternalConfig, WriteDisposition, ArrayQueryParameter, QueryJobConfig
from google.cloud.bigquery.table import RowIterator

from hca_orchestration.models.hca_dataset import TdrDataset


@dataclass
class BigQueryService:
    bigquery_client: bigquery.client.Client

    def run_query_with_destination(
            self,
            query: str,
            destination_table: str,
            bigquery_project: str
    ) -> RowIterator:
        """
        Performs a bigquery query, with no external table definitions.
        Results are deposited in the destination table provided.
        """
        job_config = bigquery.QueryJobConfig()

        job_config.destination = destination_table
        job_config.use_legacy_sql = False
        job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        query_job = self.bigquery_client.query(
            query,
            job_config,
            location='US',
            project=bigquery_project
        )

        return query_job.result()

    def run_query(
            self,
            query: str,
            bigquery_project: str,
            query_params: list[ArrayQueryParameter] = [],
            location: str = 'US'
    ) -> RowIterator:
        """
        Performs a bigquery query, with no external destination (table or otherwise)
        """
        job_config = QueryJobConfig()
        if query_params:
            job_config.query_parameters = query_params

        query_job = self.bigquery_client.query(
            query,
            job_config=job_config,
            location=location,
            project=bigquery_project
        )
        return query_job.result()

    def run_query_using_external_schema(
            self,
            query: str,
            source_paths: list[str],
            schema: Optional[list[dict[str, str]]],
            table_name: str,
            destination: str,
            bigquery_project: str
    ) -> RowIterator:
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

        query_job = self.bigquery_client.query(
            query,
            job_config,
            location='US',
            project=bigquery_project
        )

        return query_job.result()

    def build_extract_job(
            self,
            source_table: str,
            out_path: str,
            bigquery_dataset: str,
            bigquery_project: str,
            output_format: bigquery.DestinationFormat = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
    ) -> bigquery.ExtractJob:
        """
        Extracts the contents of a BQ table to the supplied out path
        """
        job_config = bigquery.job.ExtractJobConfig()
        job_config.destination_format = output_format
        job_config.print_header = False
        extract_job: bigquery.ExtractJob = self.bigquery_client.extract_table(
            f"{bigquery_dataset}.{source_table}",
            destination_uris=[
                f"{out_path}"
            ],
            job_config=job_config,
            project=bigquery_project
        )

        return extract_job.result()

    def get_num_rows_in_table(
            self,
            table_name: str,
            bigquery_dataset: str
    ) -> int:
        """
        Returns the # of rows present in the given table
        table_name should be bare, without dataset or project.
        """
        query = f"""
        SELECT COUNT(1) FROM {bigquery_dataset}.{table_name}
        """
        result = self.bigquery_client.query(query, None).result()
        cnt = -1
        for row in result:
            cnt = row[0]
        return cnt

    def build_extract_duplicates_job(
            self,
            destination_gcs_path: GsBucketWithPrefix,
            table_name: str,
            target_hca_dataset: TdrDataset,
            location: str
    ) -> RowIterator:
        """
        Returns any duplicate rows from the given table, deduping on version.
        """
        # todo much copy + paste here, refactor
        extraction_path = f"{destination_gcs_path.to_gs_path()}/*"
        if not table_name.endswith("_file"):
            query = f"""
                EXPORT DATA OPTIONS(
                    uri='{extraction_path}',
                    format='CSV',
                    overwrite=true
                ) AS
                WITH rows_ordered_by_version AS (
                    SELECT datarepo_row_id, {table_name}_id, version, rank() OVER (
                        PARTITION BY {table_name}_id ORDER BY version ASC, datarepo_row_id ASC
                    ) AS rank
                    FROM `{target_hca_dataset.project_id}.datarepo_{target_hca_dataset.dataset_name}.{table_name}`
                              ORDER BY {table_name}_id
                )
                SELECT datarepo_row_id FROM rows_ordered_by_version WHERE rank > 1;
            """
        else:
            query = f"""
                EXPORT DATA OPTIONS(
                    uri='{extraction_path}',
                    format='CSV',
                    overwrite=true
                ) AS
                WITH rows_ordered_by_version AS (
                    SELECT datarepo_row_id, {table_name}_id, version, ROW_NUMBER() OVER (
                        PARTITION BY {table_name}_id ORDER BY version DESC, file_id NULLS LAST
                    ) AS rank
                    FROM `{target_hca_dataset.project_id}.datarepo_{target_hca_dataset.dataset_name}.{table_name}`
                              ORDER BY {table_name}_id
                )
                SELECT datarepo_row_id FROM rows_ordered_by_version WHERE rank > 1;
            """

        return self.run_query(
            query,
            target_hca_dataset.project_id,
            location=location)

    def run_extract_file_ids_job(self,
                                 destination_gcs_path: GsBucketWithPrefix,
                                 table_name: str,
                                 target_hca_dataset: TdrDataset,
                                 location: str
                                 ) -> RowIterator:

        query = f"""
        EXPORT DATA OPTIONS(
            uri='{destination_gcs_path.to_gs_path()}/*',
            format='JSON',
            overwrite=true
        ) AS
        SELECT sf.{table_name}_id, sf.version, dlh.file_id, sf.content, sf.descriptor FROM `{target_hca_dataset.project_id}.datarepo_{target_hca_dataset.dataset_name}.{table_name}` sf
        LEFT JOIN  `{target_hca_dataset.project_id}.datarepo_{target_hca_dataset.dataset_name}.datarepo_load_history` dlh
            ON dlh.state = 'succeeded' AND JSON_EXTRACT_SCALAR(sf.descriptor, '$.crc32c') = dlh.checksum_crc32c
            AND '/' || JSON_EXTRACT_SCALAR(sf.descriptor, '$.file_id') || '/' || JSON_EXTRACT_SCALAR(sf.descriptor, '$.file_name') = dlh.target_path
        """

        return self.run_query(
            query,
            target_hca_dataset.project_id,
            location=location
        )
