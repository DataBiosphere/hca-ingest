import pytest
from dagster import execute_pipeline, Failure
from google.cloud.bigquery.client import Client, QueryJobConfig

from hca_orchestration.repositories.local_repository import load_hca_job
from hca_orchestration.solids.load_hca.data_files.load_data_metadata_files import NullFileIdException


@pytest.mark.e2e
def test_load_null_file_refs(load_hca_run_config, dataset_name, tdr_bigquery_client, dataset_info):
    base_area_config = load_hca_run_config.copy()
    base_area_config["solids"]["pre_process_metadata"]["config"][
        "input_prefix"] = "gs://broad-dsp-monster-hca-dev-test-storage/integration/ebi_null_file_refs/test_data"
    job = load_hca_job()

    with pytest.raises(NullFileIdException):
        execute_pipeline(
            job,
            run_config=load_hca_run_config
        )

    bq_project = dataset_info.dataset_data_project_id
    assert_no_null_file_refs("analysis_file", dataset_name, bq_project, tdr_bigquery_client)


def assert_no_null_file_refs(metadata_type: str, dataset_name: str, bq_project: str, tdr_bigquery_client: Client):
    rows = _query_metadata_table(
        metadata_type,
        dataset_name,
        bq_project,
        tdr_bigquery_client
    )
    for row in rows:
        assert row["file_id"], "Should be no null file refs"


def _query_metadata_table(metadata_type: str, dataset_name: str, bq_project: str, client: Client):
    query = f"""
    SELECT * FROM `datarepo_{dataset_name}.{metadata_type}`
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
