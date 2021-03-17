import unittest

from dagster.core.execution.context.init import InitResourceContext
import slack.web.client

from hca_orchestration.resources import live_slack_client


class LiveSlackResourceTestCase(unittest.TestCase):
    # basic test to make sure we're passing valid default configuration into the resource
    def test_resource_can_be_initialized(self):
        resource_context = InitResourceContext(resource_config={}, resource_def=live_slack_client)
        client_instance = live_slack_client.resource_fn(resource_context)
        self.assertIsInstance(client_instance, slack.web.client.WebClient)
