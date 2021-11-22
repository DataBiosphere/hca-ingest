import os

from dagster import file_relative_path, Partition
from dagster.utils import load_yaml_from_path
from dagster_utils.typing import DagsterObjectConfigSchema


def run_config_for_real_prod_migration_dcp1(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join("./run_config", "dcp1_real_prod_migration.yaml")
    )
    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)
    run_config["resources"]["hca_project_copying_config"]["config"]["source_hca_project_id"] = partition.value

    return run_config


def run_config_for_real_prod_migration_dcp2(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join("./run_config", "dcp2_real_prod_migration.yaml")
    )
    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)
    run_config["resources"]["hca_project_copying_config"]["config"]["source_hca_project_id"] = partition.value

    return run_config
