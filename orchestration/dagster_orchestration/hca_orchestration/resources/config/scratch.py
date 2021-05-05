from dataclasses import dataclass

from dagster import InitResourceContext, resource, String


@dataclass
class ScratchConfig:
    scratch_bucket_name: str
    scratch_prefix_name: str


@resource({
    "scratch_bucket_name": String,
    "scratch_prefix_name": String,
})
def scratch_config(init_context: InitResourceContext) -> ScratchConfig:
    return ScratchConfig(**init_context.resource_config)
