import uuid

from dagster import resource, Field, InitResourceContext, Bool, String, Failure

from hca_orchestration.contrib.dagster import short_run_id


@resource({
    "load_tag_prefix": Field(String),
    "append_run_id": Field(Bool),
})
def load_tag(init_context: InitResourceContext) -> str:
    """
    Generates a load tag for the pipeline, optionally suffixing
    with a run ID.

    NOTE: We can only use pipeline-level, static items when generating the load tag
    (i.e., run_id) as this will be regenerated every time we cross
    process boundaries (i.e., when running via the multiprocess executor)

    Hence, we cannot use a timestamp or other such dynamically generated data
    :return: The generated load tag
    """
    tag = f"{init_context.resource_config['load_tag_prefix']}"
    if init_context.resource_config['append_run_id']:
        tag = f"{tag}_{short_run_id(init_context.run_id)}"

    return tag
