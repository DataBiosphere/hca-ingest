from dagster import pipeline, repository, ModeDefinition
from hca_proto.solids import clear_staging_dir, pre_process_metadata
from hca_proto.resources import dataflow_beam_runner, local_beam_runner

prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "beam_runner": dataflow_beam_runner
    }
)

dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "beam_runner": local_beam_runner
    }
)


@pipeline(
    mode_defs=[prod_mode, dev_mode]
)
def stage_data():
    pre_process_metadata(clear_staging_dir())


@repository
def hca_proto():
    return [stage_data]
