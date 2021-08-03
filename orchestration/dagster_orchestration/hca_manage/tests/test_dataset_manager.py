import unittest
from unittest.mock import MagicMock, Mock, call

from data_repo_client import RepositoryApi

from hca_manage.dataset import DatasetManager, _validate_dataset_name, InvalidDatasetNameException


class DatasetManagerTestCase(unittest.TestCase):
    def setUp(self):
        self.manager = DatasetManager(
            'dev',
            MagicMock(spec=RepositoryApi, instance=True)
        )

    def test_delete_dataset_fetches_id_if_missing(self):
        enumerate_datasets_response = Mock()
        single_dataset = Mock()
        single_dataset.id = 'abc'
        enumerate_datasets_response.items = [single_dataset]
        self.manager.data_repo_client.enumerate_datasets.return_value = enumerate_datasets_response

        self.manager.delete_dataset(dataset_name='steve')

        self.manager.data_repo_client.delete_dataset.assert_called_once_with('abc', _request_timeout=30)

    def test_delete_dataset_uses_id_if_provided(self):
        self.manager.delete_dataset(dataset_id='steve')

        self.manager.data_repo_client.delete_dataset.assert_called_once_with('steve', _request_timeout=30)

    def test_delete_dataset_blows_up_if_both_or_neither_id_and_name(self):
        with self.assertRaises(ValueError):
            self.manager.delete_dataset(dataset_id='steve', dataset_name='also steve')

        with self.assertRaises(ValueError):
            self.manager.delete_dataset()

    def test_create_dataset_with_policy(self):
        self.manager.data_repo_client.create_dataset = Mock()
        self.manager.data_repo_client.retrieve_job_result = Mock(return_value={"id": "fake_dataset_id"})

        self.manager.create_dataset_with_policy_members(
            "example",
            "fake_billing_id",
            {"abc@example.com", "def@example.com"},
            {"fake": "schema"},
            "us-east4",
            "dev",
            None
        )

        self.manager.data_repo_client.create_dataset.assert_called_once()
        calls = [
            call('fake_dataset_id', policy_name='steward', policy_member={'email': 'monster@firecloud.org'}),
            call('fake_dataset_id', policy_name='steward', policy_member={'email': 'abc@example.com'}),
            call('fake_dataset_id', policy_name='steward', policy_member={'email': 'def@example.com'}),
        ]
        self.manager.data_repo_client.add_dataset_policy_member.assert_has_calls(calls, any_order=True)

    def test_valid_dataset_name(self):
        _validate_dataset_name(
            "hca_dev_20200812_dssAllNoData"
        )

    def test_invalid_dataset_name(self):
        with self.assertRaises(InvalidDatasetNameException):
            _validate_dataset_name(
                "fake_dataset_name"
            )
