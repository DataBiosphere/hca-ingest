from dagster import resource, Field, StringSource, InitResourceContext, BoolSource
from datetime import datetime


@resource({
    "load_tag_prefix": Field(StringSource),
    "append_timestamp": Field(BoolSource),
})
def load_tag(init_context: InitResourceContext) -> str:
    """
    Generates a load tag for the pipeline, optionally suffixing
    with a timestamp.
    :return: The generated load tag
    """
    tag = f"{init_context.resource_config['load_tag_prefix']}"
    if init_context.resource_config['append_timestamp']:
        tag = f"{tag}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    return tag
