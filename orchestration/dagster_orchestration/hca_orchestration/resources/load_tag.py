from datetime import datetime

from dagster import resource, Field, InitResourceContext, Bool, String


@resource({
    "load_tag_prefix": Field(String),
    "append_timestamp": Field(Bool),
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
