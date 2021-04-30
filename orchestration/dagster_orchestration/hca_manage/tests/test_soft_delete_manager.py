from io import BufferedIOBase
from tempfile import NamedTemporaryFile
import unittest
from unittest.mock import MagicMock, patch

from data_repo_client import RepositoryApi

from hca_manage.soft_delete import SoftDeleteManager
from hca_orchestration.tests.support.matchers import ObjectOfType


class SoftDeleteManagerTestCase(unittest.TestCase):
    def setUp(self):
        self.manager = SoftDeleteManager(
            environment='dev',
            data_repo_client=MagicMock(autospec=RepositoryApi),
            project='project-id',
            dataset='datasetname',
        )

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