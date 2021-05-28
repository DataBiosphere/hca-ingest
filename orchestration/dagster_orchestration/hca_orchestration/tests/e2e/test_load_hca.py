import logging
import uuid

import pytest
from dagster import execute_pipeline

from hca_manage.common import data_repo_host, get_api_client
from hca_manage.dataset import DatasetManager
from hca_orchestration.pipelines import load_hca


@pytest.mark.e2e
def test_load_hca(load_hca_run_config):
    execute_pipeline(
        load_hca,
        mode="local",
        run_config=load_hca_run_config
    )


@pytest.fixture
def dataset_name() -> str:
    return f"monster_hca_test_{str(uuid.uuid4()).replace('-', '_')}"


@pytest.fixture
def dataset_id(dataset_name) -> str:
    data_repo_client = get_api_client(data_repo_host["dev"])
    dataset_manager = DatasetManager("dev", data_repo_client)
    dataset_id = dataset_manager.create_dataset_with_policy_members(
        dataset_name,
        "390e7a85-d47f-4531-b612-165fc977d3bd",
        None,
        dataset_manager.generate_schema()
    )

    yield dataset_id
    logging.info(f"Deleting dataset, name = {dataset_name}, id = {dataset_id}")
    dataset_manager.delete_dataset(
        dataset_id=dataset_id
    )


@pytest.fixture
def load_hca_run_config(dataset_name, dataset_id):
    return {
        "resources": {
            "beam_runner": {
                "config": {
                    "working_dir": "../..",
                    "target_class": "hca-transformation-pipeline"
                }
            },
            "load_tag": {
                "config": {
                    "load_tag_prefix": "monster_test",
                    "append_timestamp": True
                }
            },
            "scratch_config": {
                "config": {
                    "scratch_bucket_name": "broad-dsp-monster-hca-dev-staging-storage",
                    "scratch_prefix_name": f"{dataset_name}",
                    "scratch_bq_project": "broad-dsp-monster-hca-dev",
                    "scratch_dataset_prefix": f"e2e_test_{dataset_name}",
                    "scratch_table_expiration_ms": 86400000
                }
            },
            "target_hca_dataset": {
                "config": {
                    "dataset_name": dataset_name,
                    "dataset_id": dataset_id,
                    "project_id": "broad-jade-dev-data",
                    "billing_profile_id": "390e7a85-d47f-4531-b612-165fc977d3bd",
                }
            }
        },
        "solids": {
            "pre_process_metadata": {
                "config": {
                    "input_prefix": "gs://broad-dsp-monster-hca-dev-test-storage/integration/ebi_micro/test_data"
                }
            }
        }
    }
