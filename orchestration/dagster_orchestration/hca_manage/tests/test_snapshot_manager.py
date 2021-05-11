import unittest
from unittest.mock import MagicMock, Mock

from dagster_utils.testing.matchers import StringMatchingRegex, ObjectWithAttributes
from data_repo_client import RepositoryApi, SnapshotRequestModel

from hca_manage.snapshot import SnapshotManager


class SnapshotManagerTestCase(unittest.TestCase):
    def setUp(self):
        self.manager = SnapshotManager(
            environment='dev',
            data_repo_client=MagicMock(autospec=RepositoryApi),
            dataset='datasetname',
        )

    def test_submit_snapshot_request_ignores_qualifier_if_not_present(self):
        self.manager.submit_snapshot_request(qualifier=None)

        self.manager.data_repo_client.create_snapshot.assert_called_once_with(
            snapshot=ObjectWithAttributes(
                SnapshotRequestModel,
                name=StringMatchingRegex(r'datasetname___\d{8}'))
        )

    def test_submit_snapshot_request_uses_qualifier_if_present(self):
        self.manager.submit_snapshot_request(qualifier='steve')

        self.manager.data_repo_client.create_snapshot.assert_called_once_with(
            snapshot=ObjectWithAttributes(
                SnapshotRequestModel,
                name=StringMatchingRegex(r'datasetname___\d{8}_steve'))
        )

    def test_delete_snapshot_fetches_id_if_missing(self):
        enumerate_snapshots_response = Mock()
        single_snapshot = Mock()
        single_snapshot.id = 'abc'
        enumerate_snapshots_response.items = [single_snapshot]
        self.manager.data_repo_client.enumerate_snapshots.return_value = enumerate_snapshots_response

        self.manager.delete_snapshot(snapshot_name='steve')

        self.manager.data_repo_client.delete_snapshot.assert_called_once_with('abc')

    def test_delete_snapshot_uses_id_if_provided(self):
        self.manager.delete_snapshot(snapshot_id='steve')

        self.manager.data_repo_client.delete_snapshot.assert_called_once_with('steve')

    def test_delete_snapshot_blows_up_if_both_or_neither_id_and_name(self):
        with self.assertRaises(ValueError):
            self.manager.delete_snapshot(snapshot_id='steve', snapshot_name='also steve')

        with self.assertRaises(ValueError):
            self.manager.delete_snapshot()
