
# get list of all assets where snapshot_name ends in {release_tag}
# for each asset:
# load into config a la manifest.py
# you will need to add the prod & dev repository.py files for the pipeline to show up in dagit

from hca_orchestration.solids.create_snapshot import (
    get_completed_snapshot_info,
)
def get_snapshot_names()

Examples from Dagster
def get_asset_partitions():
    """All snapshots where snapshot_name ends in {release_tag}"""
    return [Partition(f"<snapshot_name filtered for source dataset> ") for snapshot_name in get_snapshot_names()]


def run_config_for_date_partition(partition):
    snapshot = partition.value
    return {"solids": {"get_completed_snapshot_info": {"config": {"date": date}}}}


date_partition_set = PartitionSetDefinition(
    name="date_partition_set",
    pipeline_name="my_data_pipeline",
    partition_fn=get_date_partitions,
    run_config_fn_for_partition=run_config_for_date_partition,
)


Testing:
from dagster import validate_run_config


def test_my_partition_set():
    for partition in date_partition_set.get_partitions():
        run_config = date_partition_set.run_config_for_partition(partition)
        assert validate_run_config(my_data_pipeline, run_config)