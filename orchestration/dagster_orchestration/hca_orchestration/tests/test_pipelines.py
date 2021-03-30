import os
import pytest
import unittest
from unittest.mock import patch
import uuid
from typing import Any


from dagster import execute_pipeline, file_relative_path, PipelineDefinition, PipelineExecutionResult
from dagster.utils import load_yaml_from_globs
from dagster.utils.merger import deep_merge_dicts
from hca_orchestration.pipelines import stage_data, validate_egress
from hca_manage.diff_dirs import diff_dirs
from hca_orchestration.support.typing import DagsterConfigDict


def config_path(relative_path: str) -> str:
    path: str = file_relative_path(
        __file__, os.path.join("../environments/", relative_path)
    )
    return path


def beam_runner_path() -> str:
    path: str = file_relative_path(__file__, '../../../../')
    return path


class PipelinesTestCase(unittest.TestCase):
    def run_pipeline(self, pipeline: PipelineDefinition, config_name: str, extra_config: dict[str, Any] = {},
                     pipeline_mode='test') -> PipelineExecutionResult:
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
    def test_stage_data_local_e2e(self):
        test_id = f'test-{uuid.uuid4()}'
        config = load_yaml_from_globs(config_path('stage_data_local_e2e.yaml'))
        runtime_config = {
            'resources': {
                'beam_runner': {
                    'config': {
                        'working_dir': beam_runner_path()
                    }
                }
            },
            'solids': {
                'clear_staging_dir': {
                    'config': {
                        'staging_prefix_name': f'local-stage-data/{test_id}'
                    }
                },
                'pre_process_metadata': {
                    'config': {
                        'staging_prefix_name': f'local-stage-data/{test_id}'
                    }
                }
            }
        }

        import pdb
        pdb.set_trace()
        self.run_pipeline(stage_data, 'stage_data_local_e2e.yaml', extra_config=runtime_config, pipeline_mode='local')

        expected_blobs, output_blobs = diff_dirs(
            'broad-dsp-monster-hca-dev',
            'broad-dsp-monster-hca-dev-test-storage',
            'integration/ebi_small/expected_output',
            'broad-dsp-monster-hca-dev-temp-storage',
            f'local-stage-data/{test_id}',
        )
        self.assertEqual(expected_blobs, output_blobs, "Output results differ from expected")

    def test_stage_data_noop_resources(self):
        result = self.run_pipeline(stage_data, config_name="test_stage_data.yaml")

        self.assertTrue(result.success)

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
