from dagster import solid, Nothing, InputDefinition, ExpectationResult, String


@solid(
    # config_schema={"gcs_prefix": str}
)
def clear_staging_dir(context) -> Nothing:
    # TODO this is POC placeholder, we need to actually clear the staging dir
    # context.log.info(f"--clear_staging_dir gcs_prefix={context.solid_config['gcs_prefix']}")
    pass


@solid(
    input_defs=[InputDefinition("start", Nothing)],
    config_schema={
        "input_prefix": String,
        "output_prefix": String

    },
    required_resource_keys={"beam_runner"}
)
def pre_process_metadata(context) -> Nothing:
    context.log.info(f"--pre_process_metadata")
    input_prefix = context.solid_config["input_prefix"]
    output_prefix = context.solid_config["output_prefix"]

    yield ExpectationResult(
        success=(input_prefix != output_prefix),
        label="input_prefix_ne_output_prefix",
        description="Check that input prefix differs from output prefix"
    )
    context.resources.beam_runner.run("pre-process-metadata", input_prefix, output_prefix, context)
