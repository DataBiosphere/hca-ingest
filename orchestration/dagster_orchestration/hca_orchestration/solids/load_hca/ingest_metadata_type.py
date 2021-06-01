from typing import Iterator

from dagster import Any, Field, solid
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster.experimental import DynamicOutput, DynamicOutputDefinition

from hca_orchestration.support.typing import HcaScratchDatasetName, MetadataTypeFanoutResult


@solid(
    config_schema={
        "metadata_types": Field(Any, is_required=True),
        "prefix": Field(str, is_required=True)
    },
    output_defs=[
        DynamicOutputDefinition(name="table_fanout_result", dagster_type=MetadataTypeFanoutResult)
    ]
)
def ingest_metadata_type(context: AbstractComputeExecutionContext,
                         scratch_dataset_name: HcaScratchDatasetName) -> Iterator[MetadataTypeFanoutResult]:
    """
    For each file type, return a dynamic output over which we can later map
    This saves us from hardcoding solids for each file type
    """
    for file_metadata_type in context.solid_config["metadata_types"]:
        yield DynamicOutput(
            value=MetadataTypeFanoutResult(
                scratch_dataset_name,
                file_metadata_type.value,
                context.solid_config["prefix"]),
            mapping_key=file_metadata_type.value,
            output_name="table_fanout_result"
        )
