from dagster import solid, InputDefinition
from dagster.core.execution.context.compute import (
    AbstractComputeExecutionContext,
)


@solid(
    input_defs=[InputDefinition("entity_types", set[str])]
)
def delete_outdated_tabular_data(context: AbstractComputeExecutionContext, entity_types: set[str]) -> None:
    pass
