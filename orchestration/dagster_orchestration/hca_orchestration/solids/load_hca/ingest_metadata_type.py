from typing import Iterator

from dagster import Any, Field, String, solid
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster.experimental import DynamicOutput, DynamicOutputDefinition

from hca_orchestration.support.typing import HcaScratchDatasetName, MetadataTypeFanoutResult


@solid(
    config_schema={
        "metadata_types": Field(Any, is_required=True),
        "path": Field(String, is_required=True)
    },
    output_defs=[
        DynamicOutputDefinition(name="table_fanout_result", dagster_type=MetadataTypeFanoutResult)
    ]
)
def ingest_metadata_type(context: AbstractComputeExecutionContext,
                         scratch_dataset_name: HcaScratchDatasetName) -> Iterator[MetadataTypeFanoutResult]:
    """
    For each metadata type, return a dynamic output over which we can later map
    This saves us from hardcoding solids for each type
    """
    for metadata_type in context.solid_config["metadata_types"]:
        yield DynamicOutput(
            value=MetadataTypeFanoutResult(scratch_dataset_name, metadata_type.value, context.solid_config["path"]),
            mapping_key=metadata_type.value,
            output_name="table_fanout_result"
        )
