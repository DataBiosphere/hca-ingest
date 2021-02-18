import unittest

import os
from dagster import execute_pipeline, file_relative_path
from dagster.utils import load_yaml_from_globs
from hca_proto.pipelines import stage_data


def config_path(relative_path):
    return file_relative_path(
        __file__, os.path.join("../environments/", relative_path)
    )


class PipelinesTestCase(unittest.TestCase):
    def test_stage_data(self):
        """
        Simple example of running a pipeline with "noop" resources
        TODO Build a 'real' E2E pipeline invocation that runs
        against GS and a local Beam runner
        """
        config_dict = load_yaml_from_globs(
            config_path("test_base.yaml")
        )
        result = execute_pipeline(
            stage_data,
            mode='test',
            run_config=config_dict
        )

        self.assertTrue(result.success)


if __name__ == '__main__':
    unittest.main()
