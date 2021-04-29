import os
import unittest
from unittest import mock

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.resources import live_slack_client
from hca_orchestration.resources.slack import LiveSlackClient
from hca_orchestration.tests.support.resources import initialize_resource


class LiveSlackResourceTestCase(unittest.TestCase):
    @mock.patch.dict(os.environ, {
        **os.environ,
        'SLACK_TOKEN': 'jeepers',
    })
    def test_resource_can_be_initialized(self):
        with initialize_resource(preconfigure_resource_for_mode(live_slack_client, "test")) as client_instance:
            self.assertIsInstance(client_instance, LiveSlackClient)
