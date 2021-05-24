import csv
from io import StringIO
import unittest
from unittest.mock import MagicMock, call, patch

from dagster_utils.testing.matchers import StringContaining
from data_repo_client import RepositoryApi

from hca_manage.check import CheckManager
from hca_manage.common import populate_row_id_csv


class CheckManagerTestCase(unittest.TestCase):
    def setUp(self):
        self.manager = CheckManager(
            environment='dev',
            data_repo_client=MagicMock(spec=RepositoryApi),
            project='project-id',
            dataset='datasetname',
        )

    def test__hit_bigquery_gets_first_column_from_results(self):
        results_list = [
            ['a', 'b'],
            ['c', 'd'],
            ['e', 'f']
        ]
        with patch('google.cloud.bigquery.Client.query', return_value=results_list):
            self.assertEqual(self.manager.duplicate_manager._hit_bigquery("querying querulously"), {'a', 'c', 'e'})

    def test_populate_row_id_csv_writes_to_csv(self):
        string_io = StringIO()
        strings = {'abc', 'def', 'ghi'}

        populate_row_id_csv(strings, string_io)

        # move to the start so we can read the newly-written text
        string_io.seek(0)
        csv_reader = csv.reader(string_io)
        self.assertEqual(set(row[0] for row in csv_reader), strings)

    def test_process_rows_returns_zero_if_no_bad_rids(self):
        result = self.manager.duplicate_manager._check_or_delete_rows(
            lambda: {'table_a', 'table_b'},
            lambda table: set(),
            soft_delete=True,
            issue="oh dear")

        self.assertEqual(result, 0)

    def test_process_rows_does_no_soft_delete_if_soft_delete_false(self):
        with patch('hca_manage.soft_delete.SoftDeleteManager.soft_delete_rows') as mock_soft_delete:
            result = self.manager.duplicate_manager._check_or_delete_rows(
                lambda: {'table_a', 'table_b'},
                lambda table: {'abc'},
                soft_delete=False,
                issue="oh dear")

            mock_soft_delete.assert_not_called()
        self.assertEqual(result, 2)  # one error per table

    def test_process_rows_does_soft_delete_if_soft_delete_true(self):
        with patch('hca_manage.soft_delete.SoftDeleteManager.soft_delete_rows') as mock_soft_delete:
            result = self.manager.duplicate_manager._check_or_delete_rows(
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
