import os
import unittest
import uuid
from typing import Any
from unittest.mock import patch

import pytest
from dagster import execute_pipeline, file_relative_path, PipelineDefinition, PipelineExecutionResult
from dagster.utils import load_yaml_from_globs
from dagster.utils.merger import deep_merge_dicts

from hca_manage.diff_dirs import diff_dirs
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

    @pytest.mark.e2e
    def test_load_hca_local_e2e(self):
        test_id = f'test-{uuid.uuid4()}'
        runtime_config = {
            'resources': {
                'beam_runner': {
                    'config': {
                        'working_dir': beam_runner_path()
                    }
                },
                'scratch_config': {
                    'config': {
                        'scratch_prefix_name': f'local-stage-data/{test_id}'
                    }
                }
            }
        }

        self.run_pipeline(
            load_hca,
            'test_load_hca_local_e2e.yaml',
            extra_config=runtime_config,
            pipeline_mode='local')

        expected_blobs, output_blobs = diff_dirs(
            'broad-dsp-monster-hca-dev',
            'broad-dsp-monster-hca-dev-test-storage',
            'integration/ebi_small/expected_output',
            'broad-dsp-monster-hca-dev-temp-storage',
            f'local-stage-data/{test_id}',
        )
        self.assertEqual(expected_blobs, output_blobs, "Output results differ from expected")

    def test_load_data_noop_resources(self):
        result = self.run_pipeline(load_hca, config_name="test_load_hca_noop_resources.yaml")

        self.assertTrue(result.success)
        scratch_dataset_name = result.result_for_solid("create_scratch_dataset").output_value("result")
        self.assertTrue(
            scratch_dataset_name.startswith("fake_bq_project.testing_dataset_prefix_fake_load_tag"),
            "staging dataset should start with load tag prefix"
        )

    @patch("hca_manage.manage.HcaManage.get_null_filerefs")
    @patch("hca_manage.manage.HcaManage.get_file_table_names")
    @patch("hca_manage.manage.HcaManage.get_duplicates")
    @patch("hca_manage.manage.HcaManage.get_all_table_names")
    @patch("hca_manage.manage.HcaManage.get_dangling_proj_refs")
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
