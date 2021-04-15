import csv
from io import BufferedIOBase, BytesIO, StringIO
from tempfile import NamedTemporaryFile
import unittest
from unittest.mock import call, MagicMock, Mock, patch

from data_repo_client import RepositoryApi, DataDeletionRequest, SnapshotRequestModel

from hca_manage.manage import HcaManage
from hca_orchestration.tests.support.gcs import FakeGCSClient
from hca_orchestration.tests.support.matchers import StringContaining, StringMatchingRegex,\
    ObjectOfType, ObjectWithAttributes


class HcaManageTestCase(unittest.TestCase):
    def setUp(self):
        self.manager = HcaManage(
            'dev',
            MagicMock(autospec=RepositoryApi),
            'project-id',
            'datasetname',
        )

    def test__hit_bigquery_gets_first_column_from_results(self):
        results_list = [
            ['a', 'b'],
            ['c', 'd'],
            ['e', 'f']
        ]
        with patch('google.cloud.bigquery.Client.query', return_value=results_list):
            self.assertEqual(self.manager._hit_bigquery("querying querulously"), {'a', 'c', 'e'})

    def test_populate_row_id_csv_writes_to_csv(self):
        string_io = StringIO()
        strings = {'abc', 'def', 'ghi'}

        HcaManage.populate_row_id_csv(strings, string_io)

        # move to the start so we can read the newly-written text
        string_io.seek(0)
        csv_reader = csv.reader(string_io)
        self.assertEqual(set(row[0] for row in csv_reader), strings)

    def test_put_csv_in_bucket_puts_csv_in_bucket(self):
        fake_gcs_client = FakeGCSClient()
        target_bucket = self.manager.bucket
        expected_filename = self.manager._format_filename('some_table')

        with patch('google.cloud.storage.Client', return_value=fake_gcs_client):
            very_real_csv = BytesIO(b"steve\r\nwas\r\nhere\r\n")
            self.manager.put_csv_in_bucket(very_real_csv, 'some_table')

        expected_target_blob = fake_gcs_client.get_bucket(target_bucket).blob(expected_filename)
        expected_target_blob.upload_from_file.assert_called_once_with(very_real_csv)

    def test_process_rows_returns_zero_if_no_bad_rids(self):
        result = self.manager._process_rows(
            lambda: {'table_a', 'table_b'},
            lambda table: set(),
            soft_delete=True,
            issue="oh dear")

        self.assertEqual(result, 0)

    def test_process_rows_does_no_soft_delete_if_soft_delete_false(self):
        with patch('hca_manage.manage.HcaManage.soft_delete_rows') as mock_soft_delete:
            result = self.manager._process_rows(
                lambda: {'table_a', 'table_b'},
                lambda table: {'abc'},
                soft_delete=False,
                issue="oh dear")

            mock_soft_delete.assert_not_called()
        self.assertEqual(result, 2)  # one error per table

    def test_process_rows_does_soft_delete_if_soft_delete_true(self):
        with patch('hca_manage.manage.HcaManage.soft_delete_rows') as mock_soft_delete:
            result = self.manager._process_rows(
                lambda: {'table_a', 'table_b'},
                lambda table: {'abc'},
                soft_delete=True,
                issue="oh dear")

            mock_soft_delete.assert_has_calls(
                [
                    call(StringContaining('table_a.csv'), 'table_a'),
                    call(StringContaining('table_b.csv'), 'table_b'),
                ],
                any_order=True  # iteration order is not guaranteed for sets
            )
        self.assertEqual(result, 2)  # one error per table

    def test_soft_delete_rows_calls_submit_soft_delete(self):
        fake_google_path = 'fake://google/path.csv'
        fake_job_id = 'jorb_id'
        patched_soft_delete = patch('hca_manage.manage.HcaManage._submit_soft_delete', return_value=fake_job_id)
        patched_put_in_bucket = patch('hca_manage.manage.HcaManage.put_csv_in_bucket', return_value=fake_google_path)

        with patched_soft_delete as mock_submit, patched_put_in_bucket as mock_bucket_upload:
            with NamedTemporaryFile() as fake_file:
                returned_job_id = self.manager.soft_delete_rows(fake_file.name, 'table_a')

            mock_bucket_upload.assert_called_once_with(
                local_file=ObjectOfType(BufferedIOBase),
                target_table='table_a',
            )
            mock_submit.assert_called_once_with(target_table='table_a', target_path=fake_google_path)
            self.assertEqual(returned_job_id, fake_job_id)

    def test_get_dataset_id_gets_id_of_first_dataset(self):
        enumerate_datasets_response = Mock()
        first_dataset = Mock()
        first_dataset.id = 'abc'
        second_dataset = Mock()
        second_dataset.id = 'def'
        enumerate_datasets_response.items = [first_dataset, second_dataset]
        self.manager.data_repo_client.enumerate_datasets.return_value = enumerate_datasets_response

        self.assertEqual(self.manager.get_dataset_id(), 'abc')

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
