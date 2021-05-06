import unittest
from unittest.mock import MagicMock, Mock

from data_repo_client import RepositoryApi

from hca_manage.dataset import DatasetManager


class DatasetManagerTestCase(unittest.TestCase):
    def setUp(self):
        self.manager = DatasetManager(
            'dev',
            MagicMock(autospec=RepositoryApi)
        )

    def test_delete_dataset_fetches_id_if_missing(self):
        enumerate_datasets_response = Mock()
        single_dataset = Mock()
        single_dataset.id = 'abc'
        enumerate_datasets_response.items = [single_dataset]
        self.manager.data_repo_client.enumerate_datasets.return_value = enumerate_datasets_response

        self.manager.delete_dataset(dataset_name='steve')

        self.manager.data_repo_client.delete_dataset.assert_called_once_with('abc')

    def test_delete_dataset_uses_id_if_provided(self):
        self.manager.delete_dataset(dataset_id='steve')

        self.manager.data_repo_client.delete_dataset.assert_called_once_with('steve')

    def test_delete_dataset_blows_up_if_both_or_neither_id_and_name(self):
        with self.assertRaises(ValueError):
            self.manager.delete_dataset(dataset_id='steve', dataset_name='also steve')

        with self.assertRaises(ValueError):
            self.manager.delete_dataset()
