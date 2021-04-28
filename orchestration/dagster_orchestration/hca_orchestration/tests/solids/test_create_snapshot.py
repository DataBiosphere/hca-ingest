import unittest
from unittest.mock import patch

from dagster import execute_solid

from hca_manage.manage import JobId
from hca_orchestration.pipelines.cut_snapshot import test_mode
from hca_orchestration.solids.create_snapshot import make_snapshot_public, submit_snapshot_job


class CreateSnapshotSolidsTestCase(unittest.TestCase):
    def test_submit_snapshot_job_calls_submit_snapshot_job_in_hca_manage(self):
        with patch('hca_manage.manage.HcaManage.submit_snapshot_request_with_name', return_value=JobId("abcde")) as submit_snap:
            result = execute_solid(
                submit_snapshot_job,
                mode_def=test_mode,
                run_config={
                    'resources': {
                        'snapshot_config': {
                            'config': {
                                'dataset_name': 'badset',
                                'snapshot_name': 'namityname'
                            }
                        }
                    }
                },
            )
            self.assertTrue(result.success)
            self.assertEqual(result.output_value(), JobId('abcde'))
            submit_snap.assert_called_once_with('namityname')

    def test_make_snapshot_public_hits_correct_sam_path(self):
        with patch('hca_orchestration.resources.sam.NoopSamClient.make_snapshot_public') as mock_make_public:
            result = execute_solid(
                make_snapshot_public,
                run_config={},
                input_values={
                    'snapshot_id': 'steve',
                },
                mode_def=test_mode)
            self.assertTrue(result.success)
            mock_make_public.assert_called_once_with('steve')
