from dataclasses import dataclass
from dagster import resource, InitResourceContext


@dataclass
class ScratchConfig:
    bucket: str
    prefix: str


@resource({
    "bucket": str,
    "prefix": str
})
def scratch_config(context: InitResourceContext) -> ScratchConfig:
    if context.resource_config["bucket"].startswith("gs://"):
        raise ValueError("Bucket must not start with gs://")
    if context.resource_config["prefix"].endswith("/"):
        raise ValueError("Prefix must not end with /")

    return ScratchConfig(
        context.resource_config["bucket"],
        context.resource_config["prefix"]
    )
