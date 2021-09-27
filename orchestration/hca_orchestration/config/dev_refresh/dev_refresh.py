"""
Defines partitioning logic for the Q3 2021 dev refresh
"""

import os
import logging

from dagster import file_relative_path, Partition, PartitionSetDefinition
from dagster.utils import load_yaml_from_path
from dagster_utils.typing import DagsterObjectConfigSchema
from google.cloud.storage import Client

from hca_orchestration.contrib.dagster import gs_csv_partition_reader


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


def dev_refresh_cut_snapshot_partition_set() -> list[PartitionSetDefinition]:
    dev_refresh_partitions_path = os.environ.get("PARTITIONS_BUCKET", "")
    if not dev_refresh_partitions_path:
        logging.warning("PARTITIONS_BUCKET not set, skipping dev refresh partitioning.")
        return []

    result = gs_csv_partition_reader(dev_refresh_partitions_path, "cut_snapshot", Client(),
                                     "dev_refresh",
                                     run_config_for_cut_snapshot_partition)
    logging.warning(f"Found partitions for cut_snapshot: {result}")
    return result


def copy_project_to_new_dataset_partitions(mode: str) -> list[PartitionSetDefinition]:
    dev_refresh_partitions_path = os.environ.get("PARTITIONS_BUCKET", "")
    if not dev_refresh_partitions_path:
        logging.warning("PARTITIONS_BUCKET not set, skipping dev refresh partitioning.")
        return []

    result = gs_csv_partition_reader(dev_refresh_partitions_path, "copy_project_to_new_dataset",
                                     Client(),
                                     mode,
                                     run_config_for_per_project_dataset_partition)
    logging.warning(f"Found partitions for copy_project_to_new_dataset: {result}")
    return result
