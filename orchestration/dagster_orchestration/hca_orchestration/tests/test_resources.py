import os
import unittest
from unittest import mock

from dagster import execute_solid, ModeDefinition, solid
import slack.web.client

from hca_orchestration.resources.beam import DataflowBeamRunner
from hca_orchestration.resources import dataflow_beam_runner, live_slack_client


# n.b. 2021-03-18
# dagster support for testing resources in isolation is currently very weak
# and in active development, expect this section to use more robust tooling
# as it becomes available over the next few months
def initialize_resource(resource_def, config={}):
    # to initialize a resource, we need to initialize a context for it by setting up a mock solid
    # that requires it. details of the solid aren't important. this will get a lot more lightweight
    # as new tooling is released.
    mode = ModeDefinition(name='fake', resource_defs={'chungus': resource_def})

    @solid(required_resource_keys={'chungus'})
    def noop(context):
        return context.resources.chungus

    return execute_solid(noop, mode).output_value()


class LiveSlackResourceTestCase(unittest.TestCase):
    # basic test to make sure we're passing valid default configuration into the resource
    @mock.patch.dict(os.environ, {
        **os.environ,
        'SLACK_TOKEN': 'jeepers',
    })
    def test_resource_can_be_initialized(self):
        client_instance = initialize_resource(live_slack_client)
        self.assertIsInstance(client_instance, slack.web.client.WebClient)


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
        client_instance = initialize_resource(dataflow_beam_runner)
        self.assertIsInstance(client_instance, DataflowBeamRunner)
