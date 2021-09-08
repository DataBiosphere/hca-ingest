import os
from datetime import datetime
from google.cloud.storage.client import Client

from dagster import Partition, file_relative_path, PartitionSetDefinition
from dagster.utils import load_yaml_from_path
from dagster_utils.typing import DagsterObjectConfigSchema


def load_dcp_release_manifests():
    release_manifest_path = os.environ.get("DCP_RELEASE_MANIFEST_PATH", "")
    if not release_manifest_path:
        return []

    client = Client()
    blobs = client.list_blobs("broad-dsp-monster-hca-dev-dcp-releases", prefix="manifests")
    partition_sets = []

    for blob in blobs:
        if blob.name.endswith("_manifest.csv"):
            with blob.open("r") as manifest_handle:
                staging_paths = [
                    Partition(staging_path.strip())
                    for staging_path in manifest_handle.readlines()]
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
