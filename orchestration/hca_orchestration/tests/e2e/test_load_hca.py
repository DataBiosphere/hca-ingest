import pytest
from dagster import execute_pipeline
from google.cloud.bigquery.client import Client, QueryJobConfig

from hca_orchestration.repositories.local_repository import load_hca_job


@pytest.mark.e2e
def test_load_hca(load_hca_run_config, dataset_name, tdr_bigquery_client, dataset_info):
    job = load_hca_job()
    execute_pipeline(
        job,
        run_config=load_hca_run_config
    )

    bq_project = dataset_info.dataset_data_project_id
    assert_metadata_loaded("sequence_file", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("cell_suspension", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("collection_protocol", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("dissociation_protocol", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("donor_organism", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("library_preparation_protocol", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("process", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("project", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("sequencing_protocol", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("specimen_from_organism", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("links", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("specimen_from_organism", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("specimen_from_organism", dataset_name, bq_project, tdr_bigquery_client)
    assert_data_loaded("sequence_file", dataset_name, bq_project, tdr_bigquery_client)


def assert_data_loaded(file_type: str, dataset_name: str, bq_project: str, tdr_bigquery_client: Client):
    files_loaded = _query_files_loaded(
        file_type,
        dataset_name,
        bq_project,
        tdr_bigquery_client
    )
    assert len(files_loaded) > 0, f"Should have loaded {file_type} data files"


def assert_metadata_loaded(metadata_type: str, dataset_name: str, bq_project: str, tdr_bigquery_client: Client):
    data_loaded = _query_metadata_table(
        metadata_type,
        dataset_name,
        bq_project,
        tdr_bigquery_client
    )

    assert len(data_loaded) > 0, f"Should have loaded {metadata_type} rows"


def _query_metadata_table(metadata_type: str, dataset_name: str, bq_project: str, client: Client):
    query = f"""
    SELECT * FROM `datarepo_{dataset_name}.{metadata_type}`
    """
    return _exec_query(query, client, bq_project)


def _query_files_loaded(file_type: str, dataset_name: str, bq_project: str, client: Client):
    query = f"""
    SELECT * FROM `datarepo_{dataset_name}.{file_type}` f
    INNER JOIN `datarepo_{dataset_name}.datarepo_load_history` dlh
    ON dlh.file_id = f.file_id
    """
    return _exec_query(query, client, bq_project)


def _exec_query(query, client, bq_project):
    job_config = QueryJobConfig()
    job_config.use_legacy_sql = False

    query_job = client.query(
        query,
        job_config,
        location="US",
        project=bq_project
    )
    result = query_job.result()
    return [row for row in result]
