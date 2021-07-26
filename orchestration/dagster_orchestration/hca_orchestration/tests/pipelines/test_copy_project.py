from dagster import execute_pipeline

from hca_orchestration.pipelines.copy_project import copy_project


def test_copy_project():
    result = execute_pipeline(
        copy_project,
        mode="test",
        run_config={
            'resources': {
                'snapshot_config': {
                    'config': {
                        'dataset_name': 'badset',
                        'snapshot_name': 'namityname',
                        'managed_access': False
                    }
                }
            }
        },
    )
    assert result.success
