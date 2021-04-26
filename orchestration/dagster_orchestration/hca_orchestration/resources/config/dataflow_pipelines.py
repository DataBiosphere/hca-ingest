from dataclasses import dataclass

from dagster import InitResourceContext, resource, String


@dataclass
class StagingBucketConfig:
    staging_bucket_name: str
    staging_prefix_name: str


@resource({
    "staging_bucket_name": String,
    "staging_prefix_name": String,
})
def staging_bucket_config(init_context: InitResourceContext) -> StagingBucketConfig:
    return StagingBucketConfig(**init_context.resource_config)
