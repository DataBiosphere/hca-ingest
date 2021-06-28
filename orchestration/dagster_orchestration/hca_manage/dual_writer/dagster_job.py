from dagster import execute_pipeline

from hca_orchestration.pipelines.load_hca import load_hca


def submit_dagster_job(dataset_name: str, dataset_id: str, input_prefix: str) -> None:
    execute_pipeline(
        load_hca,
        mode="dev",
        run_config=_load_hca_run_config(dataset_id, dataset_name, input_prefix)
    )


def _load_hca_run_config(dataset_id: str, dataset_name: str, input_prefix: str) -> dict[str, object]:
    return {
        "loggers": {
            "console": {
                "config": {
                    "log_level": "INFO"
                }
            }
        },
        "resources": {
            "load_tag": {
                "config": {
                    "load_tag_prefix": "dual_write_test",
                    "append_timestamp": True
                }
            },
            "scratch_config": {
                "config": {
                    "scratch_bucket_name": "broad-dsp-monster-hca-dev-test-storage",
                    "scratch_prefix_name": f"e2e/{dataset_name}",
                    "scratch_bq_project": "broad-dsp-monster-hca-dev",
                    "scratch_dataset_prefix": f"dual_write_test_{dataset_name}",
                    "scratch_table_expiration_ms": 86400000
                }
            },
            "target_hca_dataset": {
                "config": {
                    "dataset_name": dataset_name,
                    "dataset_id": dataset_id,
                    "project_id": "broad-jade-dev-data",
                    "billing_profile_id": "390e7a85-d47f-4531-b612-165fc977d3bd"
                }
            }
        },
        "solids": {
            "pre_process_metadata": {
                "config": {
                    "input_prefix": input_prefix
                }
            }
        }
    }
