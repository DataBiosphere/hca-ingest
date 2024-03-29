import os
from datetime import datetime

from dagster import Partition, file_relative_path
from dagster.utils import load_yaml_from_path
from dagster_utils.typing import DagsterObjectConfigSchema

# isort: split

from hca_orchestration.support.matchers import find_project_id_in_str


def run_config_for_dcp_release_partition(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join("./run_config/prod", "dcp_release.yaml")
    )

    # jscpd:ignore-start
    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)
    run_config["solids"]["pre_process_metadata"]["config"]["input_prefix"] = partition.value
    creation_date = datetime.now().strftime("%Y%m%d%H%M")

    load_prefix = f"dcp_release_{creation_date}"
    run_config["resources"]["scratch_config"]["config"]["scratch_dataset_prefix"] = "staging"
    run_config["resources"]["load_tag"]["config"]["load_tag_prefix"] = load_prefix
    return run_config
    # jscpd:ignore-end


# for production releases - uses prod config
def run_config_for_dcp_release_per_project_partition(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join("./run_config/prod", "per_project_dcp_release.yaml")
    )

    # jscpd:ignore-start
    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)
    run_config["solids"]["pre_process_metadata"]["config"]["input_prefix"] = partition.value
    creation_date = datetime.now().strftime("%Y%m%d%H%M")

    load_prefix = f"dcp_release_{creation_date}"
    run_config["resources"]["scratch_config"]["config"]["scratch_dataset_prefix"] = "staging"
    run_config["resources"]["load_tag"]["config"]["load_tag_prefix"] = load_prefix

    # TODO this is kind of a hack; we're looking for a UUID in the source path and assuming it's a project ID
    project_id = find_project_id_in_str(partition.value)
    run_config["resources"]["hca_project_id"]["config"]["hca_project_id"] = project_id
    # jscpd:ignore-end

    return run_config


# for dev releases - uses dev config
# Definitely hacky and very not dry - but this is all going away, and it's how it was done before
def dev_run_config_for_dcp_release_per_project_partition(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join("./run_config/dev", "per_project_dcp_release_dev.yaml")
    )
    # jscpd:ignore-start
    # ignore that this is not DRY
    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)
    run_config["solids"]["pre_process_metadata"]["config"]["input_prefix"] = partition.value
    creation_date = datetime.now().strftime("%Y%m%d%H%M")

    load_prefix = f"dcp_release_{creation_date}"
    run_config["resources"]["scratch_config"]["config"]["scratch_dataset_prefix"] = "staging"
    run_config["resources"]["load_tag"]["config"]["load_tag_prefix"] = load_prefix

    # TODO this is kind of a hack; we're looking for a UUID in the source path and assuming it's a project ID
    project_id = find_project_id_in_str(partition.value)
    run_config["resources"]["hca_project_id"]["config"]["hca_project_id"] = project_id
    # jscpd:ignore-end

    return run_config


# run config for make_snapshot_public_job
def run_config_per_project_public_snapshot_job(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join("./run_config/prod", "per_project_public_snapshot.yaml")
    )
    # jscpd:ignore-start
    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)
    # we bake the release tag into the uploaded partitions csv (i.e, <uuid>,<release tag>)
    project_id, release_tag = partition.value.split(',')
    run_config["resources"]["snapshot_config"]["config"]["source_hca_project_id"] = project_id
    run_config["resources"]["snapshot_config"]["config"]["qualifier"] = release_tag
    # jscpd:ignore-end

    return run_config
