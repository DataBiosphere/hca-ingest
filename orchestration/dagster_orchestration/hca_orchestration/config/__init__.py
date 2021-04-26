from pkg_resources import resource_filename
from typing import Optional

from dagster import configured, ResourceDefinition
from dagster.core.definitions.configurable import ConfigurableDefinition

from hca_orchestration.support.typing import DagsterConfigDict, DagsterSolidConfigSchema
from hca_orchestration.config.preconfiguration_schema import PreconfigurationSchema


def preconfigure_for_mode(
    dagster_object: ConfigurableDefinition,
    mode_name: str,
    additional_schema: DagsterSolidConfigSchema = {},
    config_dir: Optional[str] = None,
) -> ConfigurableDefinition:
    """
    Preconfigures a Dagster object (such as a resource) for a given mode by setting all config values
    for the object to values found in the config files in the specified directory.

    The method will load from the specified directory under hca_orchestration/config/, looking for:
    * global.yaml
    * [mode].yaml

    Values in the mode-specific config wil supersede global config. Only one of these needs to be present.

    This method expects a value to be specified for all fields in the object's config schema. If
    a value should remain configurable after preconfiguration, list it in the `additional_schema` argument.
    If any values it expects to be specified are missing, it will raise an error. If any values for
    fields it does not expect are specified, it will record a warning and ignore those values.


    :param dagster_object: The definition for the object to be configured (e.g. a ResourceDefinition).
    :param mode_name: The name of the mode. This will determine the name of the mode-specific config file to load.
    :param additional_schema: Any config schema that is part of the Dagster object but should NOT be preconfigured.
    :param config_dir: Where to look for the config files. Defaults to the name of the Dagster object.
    :return: The Dagster object configured with the loaded values.
    """

    # set up a PreconfigurationSchema object for this dagster object, to indicate the fields
    # we expect to be preconfigured and where to find them
    definition_config_keys = dagster_object.config_schema.config_type.fields
    schema = PreconfigurationSchema(
        name=dagster_object.__name__,
        directory=resource_filename(__name__, config_dir or dagster_object.__name__),
        keys=(set(definition_config_keys) - set(additional_schema.keys()))
    )

    # we load the config in preconfigure_for_mode instead of in the @configured function to
    # ensure that any config issues cause errors upon initialization, instead of waiting until
    # we try to use the object being configured
    loaded_config = schema.load_for_mode(mode_name)

    @configured(dagster_object, additional_schema)
    def __dagster_object_config(extra_config: DagsterConfigDict) -> DagsterConfigDict:
        return {
            **loaded_config,
            **extra_config,
        }

    return __dagster_object_config


def preconfigure_resource_for_mode(
    resource: ResourceDefinition,
    mode_name: str,
    additional_schema: DagsterSolidConfigSchema = {}
) -> ResourceDefinition:
    """
    Helper function for preconfiguring resources, specifically.
    Tells preconfigure_for_mode to look in `resources/[resource name]` for config files.
    """
    return preconfigure_for_mode(resource, mode_name, additional_schema, f'resources/{resource.__name__}')
