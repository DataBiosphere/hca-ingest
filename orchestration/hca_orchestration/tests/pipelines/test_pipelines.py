import os
import unittest
from typing import Any
from unittest.mock import patch, MagicMock

from dagster import file_relative_path, ResourceDefinition, Failure, JobDefinition
from dagster.core.execution.execution_results import InProcessGraphResult
from dagster.utils import load_yaml_from_globs
from dagster.utils.merger import deep_merge_dicts
from dagster_utils.resources.beam.noop_beam_runner import noop_beam_runner
from dagster_utils.resources.bigquery import noop_bigquery_client
from dagster_utils.resources.data_repo.jade_data_repo import noop_data_repo_client
from dagster_utils.resources.google_storage import mock_storage_client
from dagster_utils.resources.sam import noop_sam_client
from dagster_utils.resources.slack import console_slack_client

from hca_orchestration.resources.config.data_repo import hca_manage_config, snapshot_creation_config
from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.pipelines import cut_snapshot, load_hca, validate_ingress_graph
from hca_orchestration.resources import load_tag
from hca_orchestration.resources import mock_bigquery_service
from hca_orchestration.resources.config.dagit import dagit_config
from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources.config.target_hca_dataset import target_hca_dataset
from hca_orchestration.resources.data_repo_service import mock_data_repo_service


def config_path(relative_path: str) -> str:
    path: str = file_relative_path(
        __file__, os.path.join("../environments/", relative_path)
    )
    return path


def beam_runner_path() -> str:
    path: str = file_relative_path(__file__, '../../../')
    return path


class PipelinesTestCase(unittest.TestCase):
    def run_pipeline(
            self,
            job: JobDefinition,
            config_name: str,
            extra_config: dict[str, Any] = {},
    ) -> InProcessGraphResult:
        config_dict = load_yaml_from_globs(
            config_path(config_name)
        )
        config_dict = deep_merge_dicts(config_dict, extra_config)

        return job.execute_in_process(
            run_config=config_dict,
        )

    @patch("hca_manage.bq_managers.NullFileRefManager.get_rows")
    @patch("hca_manage.bq_managers.NullFileRefManager.get_file_table_names")
    @patch("hca_manage.bq_managers.DuplicatesManager.get_rows")
    @patch("hca_manage.bq_managers.DuplicatesManager.get_all_table_names")
    @patch("hca_manage.bq_managers.DanglingFileRefManager.get_rows")
    @patch("hca_manage.bq_managers.CountsManager.get_rows")
    def test_load_hca_noop_resources(self, *mocks):
        #        result = self.run_pipeline(load_hca, config_name="test_load_hca_noop_resources.yaml")
        job = load_hca.to_job(
            resource_defs={
                "beam_runner": noop_beam_runner,
                "gcs": mock_storage_client,
                "data_repo_client": noop_data_repo_client,
                "bigquery_client": noop_bigquery_client,
                "load_tag": load_tag,
                "scratch_config": scratch_config,
                "target_hca_dataset": target_hca_dataset,
                "bigquery_service": mock_bigquery_service,
                "data_repo_service": mock_data_repo_service,
                "slack": console_slack_client,
                "dagit_config": preconfigure_resource_for_mode(dagit_config, "test")
            }
        )
        # config_dict = load_yaml_from_globs(
        #     config_path("test_load_hca_noop_resources.yaml")
        # )
        # result =  job.execute_in_process(run_config=config_dict)
        result = self.run_pipeline(job, config_name="test_load_hca_noop_resources.yaml")
        self.assertTrue(result.success)
        scratch_dataset_name = result.result_for_node("create_scratch_dataset").output_value("result")
        self.assertTrue(
            scratch_dataset_name.startswith("fake_bq_project.testing_dataset_prefix_fake"),
            "staging dataset should start with load tag prefix"
        )

    def test_validate_ingress_success(self):
        validate_ingress_graph.to_job(resource_defs={
            "slack": console_slack_client,
            "staging_area_validator": ResourceDefinition.mock_resource()
        })

        mock_validator = MagicMock()
        mock_validator.validate_staging_area = MagicMock(return_value=0)
        result = validate_ingress_graph.execute_in_process(
            config=load_yaml_from_globs(
                config_path("test_validate_ingress.yaml")
            ),
            resources={
                'slack': MagicMock(),
                "staging_area_validator": mock_validator
            })

        self.assertTrue(result.success)

    def test_validate_ingress_failure(self):
        validate_ingress_graph.to_job(resource_defs={
            "slack": console_slack_client,
            "staging_area_validator": ResourceDefinition.mock_resource()
        })

        mock_validator = MagicMock()
        mock_validator.validate_staging_area = MagicMock(return_value=1)

        with self.assertRaises(Failure):
            validate_ingress_graph.execute_in_process(
                config=load_yaml_from_globs(
                    config_path("test_validate_ingress.yaml")
                ),
                resources={
                    'slack': MagicMock(),
                    "staging_area_validator": mock_validator
                })

    @patch('dagster_utils.resources.data_repo.jade_data_repo.NoopDataRepoClient.add_snapshot_policy_member')
    def test_cut_snapshot(self, *mocks):
        job = cut_snapshot.to_job(
            resource_defs={
                "data_repo_client": noop_data_repo_client,
                "hca_manage_config": preconfigure_resource_for_mode(hca_manage_config, "test"),
                "sam_client": noop_sam_client,
                "slack": console_slack_client,
                "snapshot_config": snapshot_creation_config,
                "dagit_config": preconfigure_resource_for_mode(dagit_config, "test"),
            }
        )
        result = self.run_pipeline(job, config_name="test_create_snapshot.yaml")

        self.assertTrue(result.success)


if __name__ == '__main__':
    unittest.main()
