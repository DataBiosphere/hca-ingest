"""
Defines partitioning logic for the Q3 2021 dev refresh
"""

import os

from dagster import file_relative_path, Partition, PartitionSetDefinition
from dagster.utils import load_yaml_from_path
from dagster_utils.typing import DagsterObjectConfigSchema


def get_dev_refresh_partitions() -> list[Partition]:
    path = file_relative_path(
        __file__, os.path.join("./partitions/", "hca_project_ids.csv")
    )

    with open(path) as project_ids_file:
        lines = project_ids_file.readlines()
        project_ids = [Partition(project_id.strip()) for project_id in lines]

    return project_ids


def run_config_for_dev_refresh_partition(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join("./run_config", "copy_project_run_config.yaml")
    )
    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)
    run_config["resources"]["hca_project_copying_config"]["config"]["source_hca_project_id"] = partition.value
    run_config["resources"]["load_tag"]["config"]["load_tag_prefix"] = f"dev_refresh_project_{partition.value}"
    run_config["resources"]["scratch_config"]["config"]["scratch_prefix_name"] = f"project_copy_{partition.value}"

    return run_config


def run_config_for_per_project_dataset_partition(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join("./run_config", "copy_project_new_dataset_run_config.yaml")
    )
    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)
    run_config["resources"]["hca_project_copying_config"]["config"]["source_hca_project_id"] = partition.value
    run_config["resources"]["load_tag"]["config"]["load_tag_prefix"] = f"dev_refresh_project_{partition.value}"
    run_config["resources"]["scratch_config"]["config"]["scratch_prefix_name"] = f"project_copy_{partition.value}"

    return run_config


def dev_refresh_partition_set() -> PartitionSetDefinition:
    return PartitionSetDefinition(
        name="dev_refresh_partition_set",
        pipeline_name="copy_project",
        partition_fn=get_dev_refresh_partitions,
        run_config_fn_for_partition=run_config_for_dev_refresh_partition
    )


def dev_refresh_per_project_dataset_partition_set() -> PartitionSetDefinition:
    return PartitionSetDefinition(
        name="per_project_dataset_dev_refresh_partition_set",
        pipeline_name="copy_project_to_new_dataset",
        partition_fn=get_dev_refresh_partitions,
        run_config_fn_for_partition=run_config_for_per_project_dataset_partition
    )
