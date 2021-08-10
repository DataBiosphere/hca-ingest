from datetime import datetime
import pytest
from dagster import execute_pipeline
from google.cloud.bigquery import QueryJobConfig, Client

from hca_orchestration.pipelines import load_hca


@pytest.mark.e2e
def test_load_updated_staging_area(
        load_hca_run_config,
        dataset_name,
        tdr_bigquery_client,
        dataset_info
):
    base_area_config = load_hca_run_config.copy()
    base_area_config["solids"]["pre_process_metadata"]["config"][
        "input_prefix"] = "gs://broad-dsp-monster-hca-dev-test-storage/integration/ebi_micro/test_data"
    execute_pipeline(
        load_hca,
        mode="local",
        run_config=load_hca_run_config
    )
    assert_correct_version(
        "75e7414b-3333-4dd7-8ec5-d25748b35487",
        "2020-06-16T14:35:06.273000+00:00",
        dataset_name,
        dataset_info.dataset_data_project_id,
        tdr_bigquery_client)

    updated_load_hca_run_config = load_hca_run_config.copy()
    updated_load_hca_run_config["solids"]["pre_process_metadata"]["config"][
        "input_prefix"] = "gs://broad-dsp-monster-hca-dev-test-storage/integration/ebi_micro/test_data_with_updates"
    execute_pipeline(
        load_hca,
        mode="local",
        run_config=updated_load_hca_run_config
    )

    assert_correct_version(
        "75e7414b-3333-4dd7-8ec5-d25748b35487",
        "2021-08-06T14:35:06.273000+00:00",
        dataset_name,
        dataset_info.dataset_data_project_id,
        tdr_bigquery_client)


def assert_correct_version(sequence_file_id: str, expected_version: str, dataset_name: str,
                           bq_project: str, tdr_bigquery_client: Client):
    query = f"""
    SELECT * FROM `datarepo_{dataset_name}.sequence_file`
    WHERE sequence_file_id = '{sequence_file_id}'
    """

    rows = _exec_query(query, tdr_bigquery_client, bq_project)

    expected_version = datetime.fromisoformat(expected_version)
    assert len(rows) == 1
    assert rows[0]["version"] == expected_version, f"Row version should be {expected_version}"


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
