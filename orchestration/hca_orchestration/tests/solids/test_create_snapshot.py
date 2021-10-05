import unittest
from dagster_utils.resources.slack import console_slack_client
from unittest.mock import patch

from dagster import execute_solid, ModeDefinition, ResourceDefinition

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_manage.common import JobId
from hca_orchestration.pipelines.cut_snapshot import test_mode
from hca_orchestration.solids.create_snapshot import make_snapshot_public, submit_snapshot_job
from hca_orchestration.resources.config.data_repo import hca_manage_config, snapshot_creation_config

from hca_orchestration.resources.config.dagit import dagit_config


class CreateSnapshotSolidsTestCase(unittest.TestCase):
    def test_submit_snapshot_job_calls_submit_snapshot_job_in_hca_manage(self):
        with patch('hca_manage.snapshot.SnapshotManager.submit_snapshot_request_with_name',
                   return_value=JobId("abcde")) as submit_snap:
            result = execute_solid(
                submit_snapshot_job,
                mode_def=test_mode,
                run_config={
                    'resources': {
                        'snapshot_config': {
                            'config': {
                                'dataset_name': 'badset',
                                'snapshot_name': 'namityname',
                                'managed_access': False
                            }
                        }
                    },
                    'solids': {
                        'submit_snapshot_job': {
                            'config': {
                                'billing_profile_id': "fake_billing_profile_id"
                            }
                        }
                    }
                },
            )
            self.assertTrue(result.success)
            self.assertEqual(result.output_value(), JobId('abcde'))
            submit_snap.assert_called_once_with('namityname', False)

    def test_make_snapshot_public_hits_correct_sam_path(self):
        with patch('dagster_utils.resources.sam.NoopSamClient.set_public_flag') as mock_set_public:
            result = execute_solid(
                make_snapshot_public,
                run_config={},
                input_values={
                    'snapshot_id': 'steve',
                },
                mode_def=test_mode)
            self.assertTrue(result.success)
            mock_set_public.assert_called_once_with('steve', True)
