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


def run_config_per_project_snapshot_job(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join("./run_config", "per_project_snapshot.yaml")
    )
    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)

    # we bake the release tag into the uploaded partitions csv (i.e, <uuid>,<release tag>)
    project_id, release_tag = partition.value.split(',')
    run_config["resources"]["snapshot_config"]["config"]["source_hca_project_id"] = project_id
    run_config["resources"]["snapshot_config"]["config"]["qualifier"] = release_tag

    return run_config


# for dev releases - uses dev config
# Definitely hacky and very not dry - but this is all going away, and it's how it was done before
def run_config_per_project_snapshot_job_dev(partition: Partition) -> DagsterObjectConfigSchema:
    # jsdcp:ignore-start
    # ignore that this is not DRY
    path = file_relative_path(
        __file__, os.path.join("./run_config", "per_project_snapshot_dev.yaml")
    )
    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)

    # we bake the release tag into the uploaded partitions csv (i.e, <uuid>,<release tag>)
    project_id, release_tag = partition.value.split(',')
    run_config["resources"]["snapshot_config"]["config"]["source_hca_project_id"] = project_id
    run_config["resources"]["snapshot_config"]["config"]["qualifier"] = release_tag
    # jscpd:ignore-end

    return run_config
