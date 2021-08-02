from dagster import execute_pipeline

from hca_orchestration.pipelines.copy_project import copy_project


def test_copy_project() -> None:
    result = execute_pipeline(
        copy_project,
        mode="test",
        run_config={
            'resources': {
                'target_hca_dataset': {
                    'config': {
                        'billing_profile_id': 'fake_billing_profile',
                        'dataset_id': 'fake_dataset_id',
                        'dataset_name': 'fake_dataset_name',
                        'project_id': 'fake_bq_project_id'
                    },
                },
                "scratch_config": {
                    "config": {
                        "scratch_bq_project": "NA",
                        "scratch_bucket_name": "ignore",
                        "scratch_dataset_prefix": "NA",
                        "scratch_prefix_name": "ignore",
                        "scratch_table_expiration_ms": 0
                    }
                },
                "snapshot_config": {
                    "config": {
                        "snapshot_name": "foo_snapshot_name",
                        "bigquery_project_id": "fake_bq_project_id"
                    }
                },
                "hca_project_copying_config": {
                    "config": {
                        "project_id": "fake_project_id"
                    }
                }
            }
        },
    )
    assert result.success
