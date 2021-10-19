"""
Defines partitioning logic for the Q3 2021 dev refresh
"""

import os

from dagster import file_relative_path, Partition
from dagster.utils import load_yaml_from_path
from dagster_utils.typing import DagsterObjectConfigSchema


def run_config_for_dev_refresh_partition(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join("./run_config", "copy_project_run_config.yaml")
    )
    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)
    run_config["resources"]["hca_project_copying_config"]["config"]["source_hca_project_id"] = partition.value
    run_config["resources"]["load_tag"]["config"]["load_tag_prefix"] = f"dr"

    return run_config


def run_config_for_per_project_dataset_partition(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join("./run_config", "copy_project_new_dataset_run_config.yaml")
    )
    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)
    run_config["resources"]["hca_project_copying_config"]["config"]["source_hca_project_id"] = partition.value
    run_config["resources"]["load_tag"]["config"]["load_tag_prefix"] = f"dr"

    return run_config


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
