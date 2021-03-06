import unittest
from unittest.mock import MagicMock, patch

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.resources.sam import sam_client, Sam
from hca_orchestration.tests.support.resources import initialize_resource
from hca_orchestration.tests.support.matchers import StringEndingWith


class SamResourceTestCase(unittest.TestCase):
    def setUp(self):
        self.prod_sam_client = preconfigure_resource_for_mode(sam_client, "prod")

    def test_resource_can_be_initialized_without_extra_config(self):
        with initialize_resource(self.prod_sam_client) as client_instance:
            self.assertIsInstance(client_instance, Sam)

    def test_make_snapshot_public_hits_expected_url(self):
        with initialize_resource(self.prod_sam_client) as client_instance:
            mock_authorized_session = MagicMock()
            with patch('hca_orchestration.resources.sam.Sam._session', return_value=mock_authorized_session):
                client_instance.make_snapshot_public('fake-snapshot-id')
                mock_authorized_session.put.assert_called_once_with(
                    StringEndingWith('datasnapshot/fake-snapshot-id/policies/reader/public'),
                    data="true"
                )
