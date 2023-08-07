from unittest.mock import Mock, patch
from dagster import ModeDefinition, ResourceDefinition, execute_solid
from dagster_utils.resources.sam import Sam
from data_repo_client import RepositoryApi

# isort: split

from hca_manage.common import JobId
from hca_orchestration.contrib.data_repo.data_repo_service import DataRepoService
from hca_orchestration.resources.config.data_repo import (
    HcaManageConfig,
    SnapshotCreationConfig,
)
from hca_orchestration.solids.create_snapshot import (
    make_snapshot_public,
    submit_snapshot_job,
)


def test_submit_snapshot_job_calls_submit_snapshot_job_in_hca_manage():
    with patch('hca_manage.snapshot.SnapshotManager.submit_snapshot_request_with_name',
               return_value=JobId("abcde")) as submit_snap:
        result = execute_solid(
            submit_snapshot_job,
            mode_def=ModeDefinition(
                resource_defs={
                    "data_repo_client": ResourceDefinition.hardcoded_resource(Mock(spec=RepositoryApi)),
                    "data_repo_service": ResourceDefinition.hardcoded_resource(Mock(spec=DataRepoService)),
                    "sam_client": ResourceDefinition.hardcoded_resource(Mock(spec=Sam)),
                    "snapshot_config": ResourceDefinition.hardcoded_resource(
                        SnapshotCreationConfig("fake", "fake", "fake", False)),
                    "hca_manage_config": ResourceDefinition.hardcoded_resource(HcaManageConfig("dev", "fake"))
                }
            ),
            run_config={
                'solids': {
                    'submit_snapshot_job': {
                        'config': {
                            'billing_profile_id': "fake_billing_profile_id"
                        }
                    }
                }
            },
        )
        assert result.success
        assert result.output_value() == JobId('abcde')
        submit_snap.assert_called_once_with('fake', False, True)


def test_make_snapshot_public_hits_correct_sam_path():
    sam_client = Mock(Sam)
    result = execute_solid(
        make_snapshot_public,
        mode_def=ModeDefinition(resource_defs={
            "sam_client": ResourceDefinition.hardcoded_resource(sam_client)
        }),
        input_values={
            'snapshot_id': 'steve',
        })
    assert result.success
    sam_client.set_public_flag.assert_called_once_with('steve', True)
