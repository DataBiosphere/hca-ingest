import logging
import os
from dagster import Partition, file_relative_path, PartitionSetDefinition
from dagster.utils import load_yaml_from_path
from dagster_utils.typing import DagsterObjectConfigSchema
from datetime import datetime
from google.cloud.storage.client import Client

from hca_orchestration.contrib.dagster import gs_csv_partition_reader


def load_dcp_release_manifests() -> list[PartitionSetDefinition]:
    """
    Returns a list of PartitionSetDefinitions for all DCP release manifests located in PARTITIONS_BUCKET/load_hca.
    DCP_RELEASE_MANIFEST_PATH must be a Google Cloud Storage path. Release manifests must named in the following
    format:

    <dcp_release_id>_manifest.csv

    Each manifest must consist of line separated Google Cloud storage paths.

    :return: The list of DCP releases parsed from any discovered manifest files.
    """
    release_manifest_path = os.environ.get("PARTITIONS_BUCKET", "")
    if not release_manifest_path:
        logging.info("DCP_RELEASE_MANIFEST_PATH not set, skipping DCP release partitioning.")
        return []

    client = Client()
    partition_sets = gs_csv_partition_reader(
        release_manifest_path,
        "load_hca",
        client,
        "prod",
        run_config_for_dcp_release_partition)

    return partition_sets


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
