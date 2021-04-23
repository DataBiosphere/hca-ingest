import os
import unittest
from unittest import mock

import slack.web.client
from hca_orchestration.resources import live_slack_client
from hca_orchestration.tests.support.resources import initialize_resource


class LiveSlackResourceTestCase(unittest.TestCase):
    # basic test to make sure we're passing valid default configuration into the resource
    @mock.patch.dict(os.environ, {
        **os.environ,
        'SLACK_TOKEN': 'jeepers',
    })
    def test_resource_can_be_initialized(self):
        with initialize_resource(live_slack_client) as client_instance:
            self.assertIsInstance(client_instance, slack.web.client.WebClient)
