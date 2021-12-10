from dagster import resource, InitResourceContext


@resource
def run_start_time(init_context: InitResourceContext) -> int:
    return int(init_context.instance.get_run_stats(init_context.pipeline_run.run_id).start_time)
