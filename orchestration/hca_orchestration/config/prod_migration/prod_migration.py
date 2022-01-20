import os

from dagster import Partition, file_relative_path
from dagster.utils import load_yaml_from_path
from dagster_utils.typing import DagsterObjectConfigSchema


def run_config_for_real_prod_migration_dcp1(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join("./run_config", "dcp1_real_prod_migration.yaml")
    )
    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)
    run_config["resources"]["hca_project_id"]["config"]["hca_project_id"] = partition.value

    return run_config


def run_config_for_real_prod_migration_dcp2(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join("./run_config", "dcp2_real_prod_migration.yaml")
    )
    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)
    run_config["resources"]["hca_project_id"]["config"]["hca_project_id"] = partition.value

    return run_config


def run_config_cut_project_snapshot_job_real_prod_dcp2(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join("./run_config", "dcp2_real_prod_migration_snapshot.yaml")
    )
    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)
    run_config["resources"]["snapshot_config"]["config"]["source_hca_project_id"] = partition.value

    return run_config
