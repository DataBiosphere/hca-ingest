from datetime import datetime

from dagster import resource, Field, InitResourceContext, Bool, String, Failure


# constrain load tag size to avoid K8S naming issues
MAX_LOAD_TAG_LEN = 26


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

    if len(tag) > MAX_LOAD_TAG_LEN:
        raise Failure(f"Load tag must be less than {MAX_LOAD_TAG_LEN} chars")

    return tag
