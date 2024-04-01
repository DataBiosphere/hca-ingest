import pytest
import time
from dagster import execute_pipeline

from hca_orchestration.repositories.local_repository import load_hca_job
from hca_orchestration.tests.support.bigquery import assert_metadata_loaded, assert_data_loaded


@pytest.mark.e2e
def test_load_hca(load_hca_run_config, dataset_name, tdr_bigquery_client, dataset_info):
    job = load_hca_job()
    execute_pipeline(
        job,
        run_config=load_hca_run_config
    )

    time.sleep(600)  # pausing execution to allow for permissions to propagate

    bq_project = dataset_info.dataset_data_project_id
    assert_metadata_loaded("analysis_file", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("analysis_protocol", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("cell_suspension", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("collection_protocol", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("donor_organism", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("enrichment_protocol", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("library_preparation_protocol", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("process", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("project", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("specimen_from_organism", dataset_name, bq_project, tdr_bigquery_client)
    assert_metadata_loaded("links", dataset_name, bq_project, tdr_bigquery_client)
    assert_data_loaded("analysis_file", dataset_name, bq_project, tdr_bigquery_client)
