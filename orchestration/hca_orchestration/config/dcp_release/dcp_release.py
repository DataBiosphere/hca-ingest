import os
import logging
from datetime import datetime
from google.cloud.storage.client import Client

from dagster import Partition, file_relative_path, PartitionSetDefinition
from dagster.utils import load_yaml_from_path
from dagster_utils.contrib.google import parse_gs_path
from dagster_utils.typing import DagsterObjectConfigSchema


def load_dcp_release_manifests() -> list[PartitionSetDefinition]:
    """
    Returns a list of PartitionSetDefinitions for all DCP release manifests located at DCP_RELEASE_MANIFEST_PATH.
    DCP_RELEASE_MANIFEST_PATH must be a Google Cloud Storage path. Release manifests must named in the following
    format:

    <dcp_release_id>_manifest.csv

    Each manifest must consist of line separated Google Cloud storage paths.

    :return: The list of DCP releases parsed from any discovered manifest files.
    """
    release_manifest_path = os.environ.get("DCP_RELEASE_MANIFEST_PATH", "")
    if not release_manifest_path:
        logging.info("DCP_RELEASE_MANIFEST_PATH not set, skipping DCP release partitioning.")
        return []

    client = Client()
    parsed_path = parse_gs_path(release_manifest_path)
    blobs = client.list_blobs(parsed_path.bucket, prefix=parsed_path.bucket)
    partition_sets = []

    for blob in blobs:
        if not blob.name.endswith("_manifest.csv"):
            logging.warning(f"Invalid manifest file found at path {release_manifest_path}, ignoring.")
            continue

        with blob.open("r") as manifest_handle:
            staging_paths = [
                Partition(staging_path.strip())
                for staging_path in manifest_handle.readlines()
            ]
            release_id = blob.name.split("/")[-1].split('_')[0]

            # We need to bind the staging_paths to a lambda-local var ("paths") so we return
            # the proper state.
            #
            # The extra "ignoreme" is a workaround for a dagster bug where it determines whether
            # to call your partitioning function with a datetime (if it thinks you're working with
            # scheduling partitions) or None by looking at the number of parameters. > 1 forces
            # it to call us in the desired "mode"
            partition_sets.append(PartitionSetDefinition(
                name=release_id,
                pipeline_name="load_hca",
                partition_fn=lambda paths=staging_paths, ignoreme=None: paths,
                run_config_fn_for_partition=run_config_for_dcp_release_partition
            ))

    return partition_sets


def run_config_for_dcp_release_partition(partition: Partition) -> DagsterObjectConfigSchema:
    path = file_relative_path(
        __file__, os.path.join("./run_config", "dcp_release.yaml")
    )

    run_config: DagsterObjectConfigSchema = load_yaml_from_path(path)
    run_config["solids"]["pre_process_metadata"]["config"]["input_prefix"] = partition.value
    creation_date = datetime.now().strftime("%Y%m%d%H%M")

    load_prefix = f"dcp_release_{creation_date}"
    run_config["resources"]["scratch_config"]["config"]["scratch_dataset_prefix"] = "staging"
    run_config["resources"]["load_tag"]["config"]["load_tag_prefix"] = load_prefix
    return run_config
