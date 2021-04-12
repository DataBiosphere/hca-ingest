from contextlib import contextmanager
import os
import unittest
from unittest import mock

from dagster import DagsterInstance, ResourceDefinition, solid, execute_solid, ModeDefinition, SolidDefinition
from dagster.core.execution.build_resources import build_resources
import slack.web.client

from hca_orchestration.support.typing import DagsterConfigDict
from hca_orchestration.resources.beam import DataflowBeamRunner
from hca_orchestration.resources import dataflow_beam_runner, live_slack_client, load_tag


# n.b. 2021-03-22
# dagster support for testing resources in isolation is currently very weak
# and in active development, expect this section to use more robust and unchanging tooling
# as it becomes available over the next few months
@contextmanager
def initialize_resource(resource_def: ResourceDefinition, config: DagsterConfigDict = {}):
    with build_resources(
            {
                'test_resource': resource_def,
            },
            DagsterInstance.get(),
            {
                'test_resource': config
            }
    ) as resource_context:
        yield resource_context.test_resource


class LiveSlackResourceTestCase(unittest.TestCase):
    # basic test to make sure we're passing valid default configuration into the resource
    @mock.patch.dict(os.environ, {
        **os.environ,
        'SLACK_TOKEN': 'jeepers',
    })
    def test_resource_can_be_initialized(self):
        with initialize_resource(live_slack_client) as client_instance:
            self.assertIsInstance(client_instance, slack.web.client.WebClient)


class DataflowBeamRunnerTestCase(unittest.TestCase):
    @mock.patch.dict(os.environ, {
        **os.environ,
        'DATAFLOW_SUBNET_NAME': 'snubnet',
        'GCLOUD_REGION': 'ec-void1',
        'DATAFLOW_WORKER_MACHINE_TYPE': 'most-expensive-4',
        'DATAFLOW_STARTING_WORKERS': '2',  # these are marked as ints behind the scenes, but
        'DATAFLOW_MAX_WORKERS': '9999999',  # dagster handles translating them
        'HCA_KUBERNETES_SERVICE_ACCOUNT': 'all-seeing-eye@iam.zombo.com',
        'TRANSFORM_PIPELINE_IMAGE': 'dorian-gray',
        'TRANSFORM_PIPELINE_IMAGE_VERSION': '1890',
        'KUBERNETES_NAMESPACE': 'gamespace',
    })
    def test_resource_can_be_initialized(self):
        with initialize_resource(dataflow_beam_runner) as dataflow_runner:
            self.assertIsInstance(dataflow_runner, DataflowBeamRunner)


@solid(required_resource_keys={"load_tag"})
def load_tag_solid(context) -> str:
    tag: str = context.resources.load_tag
    return tag


class LoadTagTestCase(unittest.TestCase):
    def test_load_tag_with_suffix(self):
        result = execute_solid(
            load_tag_solid,
            run_config={
                "resources": {"load_tag": {"config": {
                    "load_tag_prefix": "fake_prefix",
                    "append_timestamp": True
                }}}
            },
            mode_def=ModeDefinition(resource_defs={"load_tag": load_tag})
        )
        self.assertTrue(result.output_value().startswith("fake_prefix"))

    def test_load_tag_no_suffix(self):
        result = execute_solid(
            load_tag_solid,
            run_config={
                "resources": {"load_tag": {"config": {
                    "load_tag_prefix": "fake_prefix",
                    "append_timestamp": False
                }}}
            },
            mode_def=ModeDefinition(resource_defs={"load_tag": load_tag})
        )

        self.assertEqual(result.output_value(), "fake_prefix")
