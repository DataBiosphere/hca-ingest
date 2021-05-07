import os

from dagster import ResourceDefinition
from dagster_utils.config import configurator_aimed_at_directory
from dagster_utils.typing import DagsterObjectConfigSchema


preconfigure_for_mode = configurator_aimed_at_directory(os.path.dirname(__file__))


def preconfigure_resource_for_mode(
    resource: ResourceDefinition,
    mode_name: str,
    additional_schema: DagsterObjectConfigSchema = {}
) -> ResourceDefinition:
    """
    Helper function for preconfiguring resources, specifically.
    Tells preconfigure_for_mode to look in `resources/[resource name]` for config files.
    """
    return preconfigure_for_mode(resource, mode_name, additional_schema, f'resources/{resource.__name__}')


__all__ = [
    'preconfigure_for_mode',
    'preconfigure_resource_for_mode',
]
