import os
import unittest
from typing import Any
from unittest.mock import patch

from dagster import execute_pipeline, file_relative_path, PipelineDefinition, PipelineExecutionResult
from dagster.utils import load_yaml_from_globs
from dagster.utils.merger import deep_merge_dicts

from hca_orchestration.pipelines import load_hca, validate_egress


def config_path(relative_path: str) -> str:
    path: str = file_relative_path(
        __file__, os.path.join("./environments/", relative_path)
    )
    return path


def beam_runner_path() -> str:
    path: str = file_relative_path(__file__, '../../../../')
    return path


class PipelinesTestCase(unittest.TestCase):
    def run_pipeline(
        self,
        pipeline: PipelineDefinition,
        config_name: str,
        extra_config: dict[str, Any] = {},
        pipeline_mode='test'
    ) -> PipelineExecutionResult:
        config_dict = load_yaml_from_globs(
            config_path(config_name)
        )
        config_dict = deep_merge_dicts(config_dict, extra_config)

        return execute_pipeline(
            pipeline,
            run_config=config_dict,
            mode=pipeline_mode
        )

    def test_load_data_noop_resources(self):
        result = self.run_pipeline(load_hca, config_name="test_load_hca_noop_resources.yaml")

        self.assertTrue(result.success)
        scratch_dataset_name = result.result_for_solid("create_scratch_dataset").output_value("result")
        self.assertTrue(
            scratch_dataset_name.startswith("fake_bq_project.testing_dataset_prefix_fake_load_tag"),
            "staging dataset should start with load tag prefix"
        )

    @patch("hca_manage.bq_managers.NullFileRefManager.get_rows")
    @patch("hca_manage.bq_managers.NullFileRefManager.get_file_table_names")
    @patch("hca_manage.bq_managers.DuplicatesManager.get_rows")
    @patch("hca_manage.bq_managers.DuplicatesManager.get_all_table_names")
    @patch("hca_manage.bq_managers.DanglingFileRefManager.get_rows")
    def test_validate_egress(self, *mocks):
        """
        currently validate_egress is just a thin wrapper around
        post_import_validate, so this just spins it up and sees if
        it runs at all
        """
        result = self.run_pipeline(validate_egress, config_name="test_validate_egress.yaml")

        self.assertTrue(result.success)


if __name__ == '__main__':
    unittest.main()
