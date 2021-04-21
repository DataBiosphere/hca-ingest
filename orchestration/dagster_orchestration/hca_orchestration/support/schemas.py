from dagster import String, StringSource

from hca_orchestration.support.typing import DagsterSolidConfigSchema


HCA_MANAGE_SCHEMA: DagsterSolidConfigSchema = {
    "gcp_env": StringSource,
    "dataset_name": String,
    "google_project_name": StringSource,
}

# schema_merger = Merger(
#     [
#         (list, ["append"]),
#         (dict, ["merge"]),
#         (set, [lambda _, __, base, nxt: base | nxt])  # unions sets
#     ],
#     [],  # these arrays configure fallback strategies - intentionally left blank to raise errors
#     []   # if values can't be merged (since that means they're not the same type, which shouldn't happen)
# )


# def extend_solid_config(
#     *configs: DagsterSolidConfig,
#     input_defs: list[InputDefinition] = [],
#     required_resource_keys: set[str] = set(),
#     config_schema: DagsterSolidConfigSchema = {}
# ) -> DagsterSolidConfig:
#     merger: Callable[[DagsterSolidConfig, DagsterSolidConfig], DagsterSolidConfig] = schema_merger.merge
#     all_configs = [
#         *configs,
#         DagsterSolidConfig({
#             'input_defs': input_defs,
#             'required_resource_keys': required_resource_keys,
#             'config_schema': config_schema,
#         })
#     ]
#     return reduce(merger, all_configs, {})
