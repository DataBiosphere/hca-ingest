from dagster import InitResourceContext, resource, String, Int

from hca_orchestration.models.scratch import ScratchConfig


@resource(
    required_resource_keys={"load_tag"},
    config_schema={
        "scratch_bucket_name": String,
        "scratch_bq_project": String,
        "scratch_dataset_prefix": String,
        "scratch_table_expiration_ms": Int

    })
def scratch_config(init_context: InitResourceContext) -> ScratchConfig:
    return ScratchConfig(scratch_prefix_name=init_context.resources.load_tag, **init_context.resource_config)
