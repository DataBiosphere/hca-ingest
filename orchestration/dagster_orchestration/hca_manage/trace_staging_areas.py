import argparse
import logging

from dagster_utils.contrib.google import authorized_session
from google.cloud.bigquery.client import Client
from hca_orchestration.solids.load_hca.data_files.load_data_metadata_files import FileMetadataTypes
from hca_manage.common import setup_cli_logging_format


def _analysis_file_cte(table_name: str, fully_qualified_dataset_name: str, project_id: str):
    query = f"""
      {table_name} AS (
      SELECT
          json_extract({table_name}.content, "$.file_core.file_name") AS file_name,
          dlh.source_name AS source_name
      FROM `{fully_qualified_dataset_name}.links` AS links
      JOIN unnest(json_extract_array(links.content, '$.links')) AS content_links
          ON json_extract_scalar(content_links, '$.link_type') = 'process_link'
      JOIN unnest(json_extract_array(content_links, '$.outputs')) AS outputs
          ON json_extract_scalar(outputs, '$.output_type') = '{table_name}'
      JOIN `{fully_qualified_dataset_name}.{table_name}` AS {table_name}
          ON json_extract_scalar(outputs, '$.output_id') = {table_name}.{table_name}_id
      JOIN `{fully_qualified_dataset_name}.datarepo_load_history` dlh
          ON {table_name}.file_id = dlh.file_id
      WHERE project_id = '{project_id}' AND
      dlh.state = 'succeeded'
  )""".strip()
    return query


def _trace_areas(bq_project, dataset_name: str, project_id: str):
    setup_cli_logging_format()

    data_file_tables_queries = [
        _analysis_file_cte(
            data_type.value,
            f"{bq_project}.datarepo_{dataset_name}",
            project_id
        )
        for data_type in FileMetadataTypes
    ]
    unions = [
        f"SELECT * FROM {data_type.value} " for data_type in FileMetadataTypes
    ]
    union_all = "UNION ALL\n\t".join(unions).strip()

    joined_ctes = ", ".join(data_file_tables_queries)
    query = f"""
    WITH {joined_ctes},
    all_files as (
        {union_all}
    ),
    all_staging_areas AS (
        SELECT regexp_extract(source_name, r'(gs:\/\/.*\/)data\/.*') AS staging_area FROM all_files
    )
    SELECT DISTINCT staging_area FROM all_staging_areas;


        """
    bigquery_client = Client(_http=authorized_session())
    query_job = bigquery_client.query(
        query
    )

    [
        logging.info(f"staging area = {row['staging_area']}")
        for row in query_job.result()
    ]


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dataset-id", required=True)
    parser.add_argument("-p", "--project-id", required=True)
    parser.add_argument("-b", "--bq-project", required=True)

    args = parser.parse_args()
    _trace_areas(args.bq_project, args.dataset_id, args.project_id)


if __name__ == '__main__':
    run()
