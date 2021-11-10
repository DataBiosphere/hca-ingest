import pytest
from dagster import execute_pipeline, in_process_executor, PipelineExecutionResult
from dagster_gcp.gcs import gcs_pickle_io_manager
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client
from dagster_utils.resources.google_storage import google_storage_client
from dagster_utils.resources.sam import sam_client
from dagster_utils.resources.slack import console_slack_client

from hca_manage.common import data_repo_host, get_api_client
from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.pipelines.cut_snapshot import cut_snapshot
from hca_orchestration.repositories.local_repository import load_hca_job, copy_project_to_new_dataset_job
from hca_orchestration.resources.config.dagit import dagit_config
from hca_orchestration.resources.config.data_repo import hca_manage_config, snapshot_creation_config
from hca_orchestration.resources.data_repo_service import data_repo_service
from hca_orchestration.tests.e2e.conftest import DatasetInfo
from hca_orchestration.tests.support.bigquery import assert_metadata_loaded, assert_data_loaded


def _get_data_repo_client():
    host = data_repo_host["dev"]
    return get_api_client(host=host)


@pytest.fixture
def snapshot(load_hca_run_config, dataset_info: DatasetInfo):
    load_job = load_hca_job()
    execute_pipeline(
        load_job,
        run_config=load_hca_run_config
    )

    snapshot_config = {
        "resources": {
            "snapshot_config": {
                "config": {
                    "dataset_name": dataset_info.dataset_name,
                    "managed_access": False,
                    "qualifier": None
                }
            }
        },
        "solids": {
            "add_steward": {
                "config": {
                    "snapshot_steward": "monster-dev@dev.test.firecloud.org"
                }
            }
        }
    }
    snapshot_job = cut_snapshot.to_job(
        resource_defs={
            "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
            "data_repo_service": data_repo_service,
            "gcs": google_storage_client,
            "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "dev"),
            "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "dev"),
            "sam_client": preconfigure_resource_for_mode(sam_client, "dev"),
            "slack": console_slack_client,
            "snapshot_config": snapshot_creation_config,
            "dagit_config": preconfigure_resource_for_mode(dagit_config, "dev"),
        },
        executor_def=in_process_executor
    )

    snapshot_job_result = execute_pipeline(snapshot_job, run_config=snapshot_config)
    snapshot_info = snapshot_job_result.result_for_solid(
        "get_completed_snapshot_info").materializations_during_compute[0]

    yield snapshot_info

    _get_data_repo_client().delete_snapshot(id=snapshot_info.tags["snapshot_id"])


@pytest.fixture
def copied_dataset(snapshot, copy_project_config):
    base_copy_project_config = copy_project_config.copy()
    base_copy_project_config["resources"]["hca_project_copying_config"] = {
        "config": {
            "source_bigquery_project_id": snapshot.tags['data_project'],
            "source_bigquery_region": "US",
            "source_hca_project_id": "90bf705c-d891-5ce2-aa54-094488b445c6",
            "source_snapshot_name": snapshot.tags['snapshot_name']
        }
    }
    copy_project_job = copy_project_to_new_dataset_job("dev", "dev")
    result: PipelineExecutionResult = execute_pipeline(
        copy_project_job,
        run_config=base_copy_project_config
    )
    copied_dataset = result.result_for_solid("validate_copied_dataset").materializations_during_compute[0]

    yield copied_dataset

    _get_data_repo_client().delete_dataset(id=copied_dataset.tags["dataset_id"])


@pytest.mark.e2e
def test_copy_project(copied_dataset, tdr_bigquery_client):
    copied_dataset_bq_project = copied_dataset.tags['project_id']
    copied_dataset_name = copied_dataset.tags['dataset_name']

    assert_metadata_loaded("links", copied_dataset_name, copied_dataset_bq_project, tdr_bigquery_client)
    assert_metadata_loaded("analysis_file", copied_dataset_name, copied_dataset_bq_project, tdr_bigquery_client)
    assert_metadata_loaded("analysis_protocol", copied_dataset_name, copied_dataset_bq_project, tdr_bigquery_client)
    assert_metadata_loaded("cell_suspension", copied_dataset_name, copied_dataset_bq_project, tdr_bigquery_client)
    assert_metadata_loaded("collection_protocol", copied_dataset_name, copied_dataset_bq_project, tdr_bigquery_client)
    assert_metadata_loaded("donor_organism", copied_dataset_name, copied_dataset_bq_project, tdr_bigquery_client)
    assert_metadata_loaded("enrichment_protocol", copied_dataset_name, copied_dataset_bq_project, tdr_bigquery_client)
    assert_metadata_loaded(
        "library_preparation_protocol",
        copied_dataset_name,
        copied_dataset_bq_project,
        tdr_bigquery_client)
    assert_metadata_loaded("process", copied_dataset_name, copied_dataset_bq_project, tdr_bigquery_client)
    assert_metadata_loaded("project", copied_dataset_name, copied_dataset_bq_project, tdr_bigquery_client)
    assert_metadata_loaded(
        "specimen_from_organism",
        copied_dataset_name,
        copied_dataset_bq_project,
        tdr_bigquery_client)
    assert_data_loaded("analysis_file", copied_dataset_name, copied_dataset_bq_project, tdr_bigquery_client)
