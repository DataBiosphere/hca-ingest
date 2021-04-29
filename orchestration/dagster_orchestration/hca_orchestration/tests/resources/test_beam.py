import unittest

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.resources import dataflow_beam_runner
from hca_orchestration.resources.beam import DataflowBeamRunner
from hca_orchestration.tests.support.resources import initialize_resource


class DataflowBeamRunnerTestCase(unittest.TestCase):
    def test_resource_can_be_initialized_without_extra_config(self):
        with initialize_resource(preconfigure_resource_for_mode(dataflow_beam_runner, "prod")) as dataflow_runner:
            self.assertIsInstance(dataflow_runner, DataflowBeamRunner)
