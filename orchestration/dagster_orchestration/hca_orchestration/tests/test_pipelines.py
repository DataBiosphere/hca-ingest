import os
import pytest
import unittest
from unittest.mock import patch
import uuid


from dagster import execute_pipeline, file_relative_path
from dagster.utils import load_yaml_from_globs
from hca_orchestration.pipelines import stage_data, validate_egress
from hca_manage.diff_dirs import diff_dirs


def config_path(relative_path):
    return file_relative_path(
        __file__, os.path.join("../environments/", relative_path)
    )


class PipelinesTestCase(unittest.TestCase):
    def run_pipeline(self, pipeline, config_dict, mode, *execution_args, **execution_kwargs):
        return execute_pipeline(
            pipeline,
            *execution_args,
            mode=mode,
            run_config=config_dict,
            **execution_kwargs
        )

    def run_pipeline_with_yaml_config(self, pipeline, config_name, mode='test', *execution_args, **execution_kwargs):
        config_dict = load_yaml_from_globs(
            config_path(config_name)
        )
        return self.run_pipeline(pipeline, config_dict, mode, *execution_args, **execution_kwargs)

    @pytest.mark.e2e
    def test_stage_data_local_e2e(self):
        test_id = f'test-{uuid.uuid4()}'

        config = load_yaml_from_globs(config_path('stage_data_local_e2e.yaml'))
        config['solids']['clear_staging_dir']['config']['staging_prefix_name'] = f'local-stage-data/{test_id}'
        config['solids']['pre_process_metadata']['config']['staging_prefix_name'] = f'local-stage-data/{test_id}'

        self.run_pipeline(stage_data, config, mode='local')

        expected_blobs, output_blobs = diff_dirs(
            'broad-dsp-monster-hca-dev',
            'broad-dsp-monster-hca-dev-test-storage',
            'integration/ebi_small/expected_output',
            'broad-dsp-monster-hca-dev-temp-storage',
            f'local-stage-data/{test_id}',
        )
        assert expected_blobs == output_blobs, "Output results differ from expected"

    def test_stage_data(self):
        """
        Simple example of running a pipeline with "noop" resources
        TODO Build a 'real' E2E pipeline invocation that runs
        against GS and a local Beam runner
        """
        result = self.run_pipeline_with_yaml_config(stage_data, config_name="test_stage_data.yaml")

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

        result = self.run_pipeline_with_yaml_config(validate_egress, config_name="test_validate_egress.yaml")

        self.assertTrue(result.success)


if __name__ == '__main__':
    unittest.main()
