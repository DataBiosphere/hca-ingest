"""
Defines partitioning logic for the Q3 2021 dev refresh
"""

from dagster import Partition
from dagster_utils.typing import DagsterObjectConfigSchema


def run_config_for_cut_snapshot_partition(partition: Partition) -> DagsterObjectConfigSchema:
    run_config = {
        "solids": {
            "add_steward": {
                "config": {
                    "snapshot_steward": "monster-dev@dev.test.firecloud.org"
                }
            }
        },
        "resources": {
            "snapshot_config": {
                "config": {
                    "source_hca_project_id": partition.value,
                    "managed_access": False,
                }
            }
        }
    }

    return run_config
