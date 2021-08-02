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
                "hca_project_copying_config": {
                    "config": {
                        "source_snapshot_name": "fake_source_snapshot_name",
                        "source_bigquery_project_id": "fake_source_bq_project_id",
                        "source_hca_project_id": "fake_project_id",
                        "load_tag": "fake_load_tag"
                    }
                }
            }
        },
    )
    assert result.success
