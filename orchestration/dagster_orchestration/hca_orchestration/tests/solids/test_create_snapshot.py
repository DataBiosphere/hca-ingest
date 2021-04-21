import unittest
from unittest.mock import patch
import uuid

from dagster import ModeDefinition, execute_solid
from data_repo_client import SnapshotModel

from hca_manage.manage import JobId
from hca_orchestration.resources.data_repo import noop_data_repo_client
from hca_orchestration.resources.sam import noop_sam_client
from hca_orchestration.solids.create_snapshot import make_snapshot_public, submit_snapshot_job


class CreateSnapshotSolidsTestCase(unittest.TestCase):
    def test_submit_snapshot_job_calls_submit_snapshot_job_in_hca_manage(self):
        test_mode = ModeDefinition(
            name="test",
            resource_defs={
                "data_repo_client": noop_data_repo_client,
            }
        )
        with patch('hca_manage.manage.HcaManage.submit_snapshot_request', return_value=JobId("abcde")) as submit_snap:
            result = execute_solid(
                submit_snapshot_job,
                run_config={
                    'solids': {
                        'submit_snapshot_job': {
                            'config': {
                                'gcp_env': 'dev',
                                'dataset_name': 'badset',
                                'google_project_name': 'schmloogle',
                            }
                        }
                    }
                },
                mode_def=test_mode)
            self.assertTrue(result.success)
            self.assertEqual(result.output_value(), JobId('abcde'))
            submit_snap.assert_called_once_with(None)

    def test_make_snapshot_public_hits_correct_sam_path(self):
        test_mode = ModeDefinition(
            name="test",
            resource_defs={
                "sam_client": noop_sam_client,
            }
        )
        # SnapshotModel has VERY strict restrictions on valid IDs,
        # so we need to generate an actual UUID here for it to let us
        # create an instance.
        snapshot_id = str(uuid.uuid4())

        with patch('hca_orchestration.resources.sam.NoopSamClient.make_snapshot_public') as mock_make_public:
            result = execute_solid(
                make_snapshot_public,
                run_config={},
                input_values={
                    'snapshot_info': SnapshotModel(id=snapshot_id),
                },
                mode_def=test_mode)
            self.assertTrue(result.success)
            mock_make_public.assert_called_once_with(snapshot_id)
