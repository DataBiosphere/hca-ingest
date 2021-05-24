from io import BufferedIOBase, BytesIO
from tempfile import NamedTemporaryFile
import unittest
from unittest.mock import MagicMock, Mock, patch

from dagster_utils.testing.matchers import ObjectOfType
from data_repo_client import RepositoryApi, DataDeletionRequest

from hca_manage.common import get_dataset_id
from hca_manage.soft_delete import SoftDeleteManager
from hca_orchestration.tests.support.gcs import FakeGCSClient


class SoftDeleteManagerTestCase(unittest.TestCase):
    def setUp(self):
        self.manager = SoftDeleteManager(
            environment='dev',
            data_repo_client=MagicMock(spec=RepositoryApi),
            project='project-id',
            dataset='datasetname',
        )

    def test_soft_delete_rows_calls_submit_soft_delete(self):
        fake_google_path = 'fake://google/path.csv'
        fake_job_id = 'jorb_id'
        patched_soft_delete = patch('hca_manage.soft_delete.SoftDeleteManager._submit_soft_delete',
                                    return_value=fake_job_id)
        patched_put_in_bucket = patch('hca_manage.soft_delete.SoftDeleteManager.put_soft_delete_csv_in_bucket',
                                      return_value=fake_google_path)

        with patched_soft_delete as mock_submit, patched_put_in_bucket as mock_bucket_upload:
            with NamedTemporaryFile() as fake_file:
                returned_job_id = self.manager.soft_delete_rows(fake_file.name, 'table_a')

            mock_bucket_upload.assert_called_once_with(
                local_file=ObjectOfType(BufferedIOBase),
                target_table='table_a',
            )
            mock_submit.assert_called_once_with(target_table='table_a', target_path=fake_google_path)
            self.assertEqual(returned_job_id, fake_job_id)

    def test_put_csv_in_bucket_puts_csv_in_bucket(self):
        fake_gcs_client = FakeGCSClient()
        target_bucket = self.manager.bucket
        expected_filename = self.manager._format_filename('some_table')

        with patch('google.cloud.storage.Client', return_value=fake_gcs_client):
            very_real_csv = BytesIO(b"steve\r\nwas\r\nhere\r\n")
            self.manager.put_soft_delete_csv_in_bucket(very_real_csv, 'some_table')

        expected_target_blob = fake_gcs_client.get_bucket(target_bucket).blob(expected_filename)
        expected_target_blob.upload_from_file.assert_called_once_with(very_real_csv)

    def test_get_dataset_id_gets_id_of_first_dataset(self):
        enumerate_datasets_response = Mock()
        first_dataset = Mock()
        first_dataset.id = 'abc'
        second_dataset = Mock()
        second_dataset.id = 'def'
        enumerate_datasets_response.items = [first_dataset, second_dataset]
        self.manager.data_repo_client.enumerate_datasets.return_value = enumerate_datasets_response

        self.assertEqual(get_dataset_id(dataset=self.manager.dataset, data_repo_client=self.manager.data_repo_client),
                         'abc')

    def test_submit_soft_delete_submits_dataset_deletion_with_expected_params(self):
        enumerate_datasets_response = Mock()
        single_dataset = Mock()
        single_dataset.id = 'abc'
        enumerate_datasets_response.items = [single_dataset]
        self.manager.data_repo_client.enumerate_datasets.return_value = enumerate_datasets_response

        self.manager._submit_soft_delete('table_a', 'bad_rows_a.csv')

        self.manager.data_repo_client.apply_dataset_data_deletion.assert_called_once_with(
            id='abc',
            data_deletion_request=DataDeletionRequest(
                delete_type="soft",
                spec_type="gcsFile",
                tables=[
                    {
                        "gcsFileSpec": {
                            "fileType": "csv",
                            "path": 'bad_rows_a.csv',
                        },
                        "tableName": 'table_a'
                    }
                ]
            )
        )
