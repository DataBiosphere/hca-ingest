from typing import NamedTuple

from dagster import solid, InputDefinition, Nothing
from dagster.experimental import DynamicOutput, DynamicOutputDefinition

JadeDatasetId = str
HcaStagingDatasetName = str
StagingBucket = str
StagingPrefix = str
JobId = str


class TableFanoutResult(NamedTuple):
    staging_dataset: HcaStagingDatasetName
    table_name: str


def build_table_fanout(
        tables,
        name,
        input_defs=[InputDefinition("staging_dataset", HcaStagingDatasetName)]
):
    """ Utility function that produces a dynamic fan-out over
    a supplied list of table names
    """
    @solid(
        name=name,
        input_defs=input_defs,
        output_defs=[
            DynamicOutputDefinition(name="fanout_result", dagster_type=TableFanoutResult)
        ]
    )
    def fan_out_to_tables(_, staging_dataset: HcaStagingDatasetName) -> str:
        for table in tables:
            yield DynamicOutput(value=TableFanoutResult(staging_dataset, table), mapping_key=table,
                                output_name="fanout_result")

    return fan_out_to_tables


def build_table_fanin(
        name,
        input_defs=[InputDefinition("upstream_results", Nothing)]
):
    """ When we want to consume the results of multiple fan-outs, we need to fan-in first and then
    handoff due to current dagster limitations. This utility function is a dumb passthrough to let us
    do that.
    """
    @solid(
        name=name,
        input_defs=input_defs
    )
    def fan_in(_) -> Nothing:
        pass

    return fan_in


def gs_path_from_bucket_prefix(bucket: StagingBucket, prefix: StagingPrefix):
    return f"gs://{bucket}/{prefix}"
