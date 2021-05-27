import logging
import pytest
import unittest
import uuid

from dagster import execute_pipeline

from hca_orchestration.pipelines import load_hca
from hca_manage.dataset import DatasetManager
from hca_manage.common import data_repo_host, get_api_client


class LoadHcaTest(unittest.TestCase):

    def setUp(self) -> None:
        self.data_repo_client = get_api_client(data_repo_host["dev"])
        self.dataset_manager = DatasetManager("dev", self.data_repo_client)
        self.test_name = f"monster_hca_test_{str(uuid.uuid4()).replace('-', '_')}"

        self.dataset_id = self.dataset_manager.create_dataset_with_policy_members(
            self.test_name,
            "390e7a85-d47f-4531-b612-165fc977d3bd",
            None,
            self.dataset_manager.generate_schema()
        )

    def tearDown(self) -> None:
        logging.info(f"Deleting dataset, name = {self.test_name}, id = {self.dataset_id}")
        self.dataset_manager.delete_dataset(
            dataset_id=self.dataset_id
        )

    @pytest.mark.e2e
    def test_load_hca(self):
        run_config = {
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
                        "scratch_prefix_name": f"{self.test_name}",
                        "scratch_bq_project": "broad-dsp-monster-hca-dev",
                        "scratch_dataset_prefix": f"e2e_test_{self.test_name}",
                        "scratch_table_expiration_ms": 86400000
                    }
                },
                "target_hca_dataset": {
                    "config": {
                        "dataset_name": self.test_name,
                        "dataset_id": self.dataset_id,
                        "project_id": "broad-jade-dev-data",
                        "billing_profile_id": "390e7a85-d47f-4531-b612-165fc977d3bd",
                    }
                }
            },
            "solids": {
                "pre_process_metadata": {
                    "config": {
                        "input_prefix": "gs://broad-dsp-monster-hca-dev-test-storage/integration/ebi_small/test_data"
                    }
                }
            }
        }

        execute_pipeline(
            load_hca,
            mode="local",
            run_config=run_config
        )
