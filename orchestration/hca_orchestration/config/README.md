# config

Files in this folder can be used to preconfigure resources. See the [dagster_utils docs on preconfigure_for_mode](https://github.com/broadinstitute/dagster-utils/blob/main/dagster_utils/config/configurators.py) for more information.

To add a new set of configurations for a resource, simply create a folder with the same name as the resource under the `resources` directory and add YAML files for the mode(s) you'd like to configure (named as `[mode name].yaml`). Keys in the YAML files must exactly match the setting it corresponds to in the resource's config schema. Settings that do not change based on environment can go in a file named `global.yaml`. Settings can be defined in both global and mode-specific settings files - mode-specific settings take precedent.
