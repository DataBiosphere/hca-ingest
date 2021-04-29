from dagster import solid, Nothing, InputDefinition
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

from hca_orchestration.solids.load_hca.stage_data import HcaStagingDatasetName


@solid(
    input_defs=[InputDefinition("staging_dataset_name", HcaStagingDatasetName)]
)
def import_data_files(context: AbstractComputeExecutionContext, staging_dataset_name: HcaStagingDatasetName) -> Nothing:
    # TODO this is a stub for this branch of the pipeline
    context.log.info(f"Loading files staging dataset = {staging_dataset_name}")
