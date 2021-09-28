import logging
import uuid
from dagster import Partition, PartitionSetDefinition

from google.cloud.storage import Client
from typing import Callable, Any, TypeVar, Optional

T = TypeVar("T")


def gs_csv_partition_reader(gs_partitions_bucket_name: str, pipeline_name: str, gs_client: Client, mode: str,
                            run_config_fn_for_partition: Callable[[Partition[T]], Any]) -> list[PartitionSetDefinition]:
    """
    Parses any csv files at the given GS path, interpreting each line as a separate
    partition.

    Files must end with a ".csv" suffix; the filename prefix becomes the name
    of the partition set. For example:

    foo_partitions.csv

    will create a partition set named `foo_partitions`
    :param gs_partition_csv_path: Path to the partition set files
    :param pipeline_name: Name of the pipeline to which the partition set applies
    :param gs_client: GS storage client
    :param run_config_fn_for_partition: Function that will render a valid run config for the partitions
    :return: Set of partitions
    """
    blobs = gs_client.list_blobs(gs_partitions_bucket_name, prefix=pipeline_name)
    partition_sets = []

    for blob in blobs:
        if not blob.name.endswith(".csv"):
            logging.info(f"Blob at {blob.name} is not a CSV, ignoring.")
            continue

        with blob.open("r") as parition_file_handle:
            partitions = [
                Partition(staging_path.strip())
                for staging_path in parition_file_handle.readlines()
            ]
            partition_set_id = f"{pipeline_name}_{blob.name.split('/')[-1].split('.csv')[0]}"

            logging.info(f"Partition set {partition_set_id} loaded")
            # We need to bind the staging_paths to a lambda-local var ("paths") so we return
            # the proper state.
            #
            # The extra "ignoreme" is a workaround for a dagster bug where it determines whether
            # to call your partitioning function with a datetime (if it thinks you're working with
            # scheduling partitions) or None by looking at the number of parameters. > 1 forces
            # it to call us in the desired "mode"
            partition_sets.append(PartitionSetDefinition(
                name=partition_set_id,
                pipeline_name=pipeline_name,
                partition_fn=lambda paths=partitions, ignoreme=None: paths,
                run_config_fn_for_partition=run_config_fn_for_partition,
                mode=mode
            ))

    return partition_sets


def short_run_id(run_id: Optional[str]) -> str:
    if not run_id:
        run_id = uuid.uuid4().hex
    tag = f"{run_id[0:8]}"
    return tag
