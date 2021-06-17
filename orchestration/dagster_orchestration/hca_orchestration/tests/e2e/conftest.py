import logging
import uuid
from typing import Iterable

import pytest
from google.cloud.bigquery.client import Client

from hca_manage.common import data_repo_host, get_api_client, data_repo_profile_ids
from hca_manage.dataset import DatasetManager

MONSTER_TEST_DATASET_SENTINEL = "MONSTER_TEST_DELETEME"


@pytest.fixture
def tdr_bigquery_client():
    return Client()


@pytest.fixture
def delete_dataset_on_exit():
    return True


@pytest.fixture
def existing_dataset_id():
    return None


@pytest.fixture
def dataset_name() -> str:
    return f"monster_hca_test_{str(uuid.uuid4()).replace('-', '_')}"


@pytest.fixture
def dataset_id(dataset_name, delete_dataset_on_exit, existing_dataset_id) -> Iterable[str]:
    data_repo_client = get_api_client(data_repo_host["dev"])
    dataset_manager = DatasetManager("dev", data_repo_client)

    # setup, either create the dataset or re-use the existing one if passed in as a fixture
    if existing_dataset_id:
        logging.info(f"Existing dataset ID = {existing_dataset_id}")
        logging.info("This dataset will not be deleted at the end of the test")
        dataset_id = existing_dataset_id
    else:
        logging.info("No existing dataset ID passed, creating new dataset")
        dataset_id = dataset_manager.create_dataset_with_policy_members(
            dataset_name,
            data_repo_profile_ids["dev"],
            None,
            dataset_manager.generate_schema(),
            "US",
            "dev",
            MONSTER_TEST_DATASET_SENTINEL
        )

    yield dataset_id

    # clean up
    if delete_dataset_on_exit:
        logging.info(f"Deleting dataset, name = {dataset_name}, id = {dataset_id}")
        dataset_manager.delete_dataset(
            dataset_id=dataset_id
        )
    else:
        logging.info("Leaving dataset in place, this will require manual cleanup.")
        logging.info(f"name = {dataset_name}, id = {dataset_id}")


@pytest.fixture
def load_hca_run_config(dataset_name, dataset_id):
    return {
        "loggers": {
            "console": {
                "config": {
                    "log_level": "INFO"
                }
            }
        },
        "resources": {
            "beam_runner": {
                "config": {
                    "working_dir": "../..",
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
                    "scratch_bucket_name": "broad-dsp-monster-hca-dev-test-storage",
                    "scratch_prefix_name": f"e2e/{dataset_name}",
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
                    "billing_profile_id": data_repo_profile_ids["dev"],
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
