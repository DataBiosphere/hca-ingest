import unittest

import os
from dagster import execute_pipeline, file_relative_path
from dagster.utils import load_yaml_from_globs
from hca_orchestration.pipelines import stage_data, validate_egress


def config_path(relative_path):
    return file_relative_path(
        __file__, os.path.join("../environments/", relative_path)
    )


class PipelinesTestCase(unittest.TestCase):
    def run_pipeline(self, pipeline, config_name="test_base.yaml", *execution_args, **execution_kwargs):
        config_dict = load_yaml_from_globs(
            config_path(config_name)
        )
        return execute_pipeline(
            pipeline,
            *execution_args,
            mode='test',
            run_config=config_dict,
            **execution_kwargs
        )

    def test_stage_data(self):
        """
        Simple example of running a pipeline with "noop" resources
        TODO Build a 'real' E2E pipeline invocation that runs
        against GS and a local Beam runner
        """
        result = self.run_pipeline(stage_data)

        self.assertTrue(result.success)

    def test_validate_egress(self):
        """
        currently validate_egress is just a thin wrapper around
        post_import_validate, so this just spins it up and sees if
        it runs at all
        """

        result = self.run_pipeline(validate_egress)

        self.assertTrue(result.success)


if __name__ == '__main__':
    unittest.main()
