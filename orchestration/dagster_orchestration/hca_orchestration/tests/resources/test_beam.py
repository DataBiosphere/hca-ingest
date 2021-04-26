import os
import unittest
from unittest import mock

from hca_orchestration.resources import dataflow_beam_runner
from hca_orchestration.resources.beam import DataflowBeamRunner
from hca_orchestration.tests.support.resources import initialize_resource


class DataflowBeamRunnerTestCase(unittest.TestCase):
    @mock.patch.dict(os.environ, {
        **os.environ,
        'DATAFLOW_SUBNET_NAME': 'snubnet',
        'GCLOUD_REGION': 'ec-void1',
        'DATAFLOW_WORKER_MACHINE_TYPE': 'most-expensive-4',
        'DATAFLOW_STARTING_WORKERS': '2',   # these are marked as ints behind the scenes, but
        'DATAFLOW_MAX_WORKERS': '9999999',  # dagster handles translating them
        'HCA_KUBERNETES_SERVICE_ACCOUNT': 'all-seeing-eye@iam.zombo.com',
        'TRANSFORM_PIPELINE_IMAGE': 'dorian-gray',
        'TRANSFORM_PIPELINE_IMAGE_VERSION': '1890',
        'KUBERNETES_NAMESPACE': 'gamespace',
    })
    def test_resource_can_be_initialized(self):
        with initialize_resource(dataflow_beam_runner) as dataflow_runner:
            self.assertIsInstance(dataflow_runner, DataflowBeamRunner)
