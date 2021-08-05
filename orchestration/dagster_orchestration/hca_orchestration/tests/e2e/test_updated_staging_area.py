from datetime import datetime
import pytest
from dagster import execute_pipeline
from google.cloud.bigquery import QueryJobConfig

from hca_orchestration.pipelines import load_hca


@pytest.mark.e2e
def test_load_updated_staging_area(
        load_hca_run_config,
        dataset_name,
        tdr_bigquery_client,
        dataset_info
):
    execute_pipeline(
        load_hca,
        mode="local",
        run_config=load_hca_run_config
    )

    updated_load_hca_run_config = load_hca_run_config.copy()
    updated_load_hca_run_config["solids"]["pre_process_metadata"]["config"][
        "input_prefix"] = " gs://broad-dsp-monster-hca-dev-test-storage/integration/ebi_small/updated_test_data"
    execute_pipeline(
        load_hca,
        mode="local",
        run_config=updated_load_hca_run_config
    )

    query = f"""
    SELECT * FROM `datarepo_{dataset_name}.sequence_file`
    WHERE sequence_file_id = '75e7414b-3333-4dd7-8ec5-d25748b35487'
    """

    bq_project = dataset_info.dataset_data_project_id
    rows = _exec_query(query, tdr_bigquery_client, bq_project)

    expected_version = datetime.fromisoformat("2021-07-30T09:19:00.273000+00:00")
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
