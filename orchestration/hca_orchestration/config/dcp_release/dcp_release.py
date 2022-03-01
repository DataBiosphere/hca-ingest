import os
import re
from datetime import datetime

from dagster import Partition, file_relative_path
from dagster.utils import load_yaml_from_path
from dagster_utils.typing import DagsterObjectConfigSchema


def run_config_for_dcp_release_partition(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join(f"./run_config/prod", "dcp_release.yaml")
    )

    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)
    run_config["solids"]["pre_process_metadata"]["config"]["input_prefix"] = partition.value
    creation_date = datetime.now().strftime("%Y%m%d%H%M")

    load_prefix = f"dcp_release_{creation_date}"
    run_config["resources"]["scratch_config"]["config"]["scratch_dataset_prefix"] = "staging"
    run_config["resources"]["load_tag"]["config"]["load_tag_prefix"] = load_prefix
    return run_config


def run_config_for_dcp_release_per_project_partition(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join(f"./run_config/prod", "per_project_dcp_release.yaml")
    )

    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)
    run_config["solids"]["pre_process_metadata"]["config"]["input_prefix"] = partition.value
    creation_date = datetime.now().strftime("%Y%m%d%H%M")

    load_prefix = f"dcp_release_{creation_date}"
    run_config["resources"]["scratch_config"]["config"]["scratch_dataset_prefix"] = "staging"
    run_config["resources"]["load_tag"]["config"]["load_tag_prefix"] = load_prefix

    # TODO this is kind of a hack; we're looking for a UUID in the source path and assuming it's a project ID
    uuid_matcher = re.compile('[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}', re.I)
    project_ids = uuid_matcher.findall(partition.value)

    if len(project_ids) != 1:
        raise Exception(f"Found more than one or zero project UUIDs in {partition.value}")

    run_config["resources"]["hca_project_id"]["config"]["hca_project_id"] = project_ids[0]

    return run_config
