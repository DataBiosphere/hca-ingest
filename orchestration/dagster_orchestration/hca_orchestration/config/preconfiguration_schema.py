from dataclasses import dataclass
import os
from typing import Optional
import warnings

import yaml

from hca_orchestration.support.typing import DagsterConfigDict


def load_config(config_path: str) -> Optional[DagsterConfigDict]:
    try:
        with open(config_path, 'r') as config_file_io:
            return DagsterConfigDict(yaml.safe_load(config_file_io))
    except FileNotFoundError:
        return None


@dataclass
class PreconfigurationSchema:
    name: str
    directory: str
    optional_keys: set[str]
    required_keys: set[str]

    # raises an error for any missing config keys, records a warning for (and discards) any extra keys
    def validated_config(self, config: DagsterConfigDict) -> DagsterConfigDict:
        keys_present = set(config.keys())
        permitted_keys = self.required_keys | self.optional_keys
        missing_keys = self.required_keys - keys_present
        extra_keys = keys_present - permitted_keys

        if any(missing_keys):
            missing_keys_str = ", ".join(key for key in missing_keys)
            raise ValueError(
                f"Missing expected preconfigured field(s) in configuration files for {self.name}. "
                f"Config files do not define these required fields:\n{missing_keys_str}\n(config dir: {self.directory})"
            )

        if any(extra_keys):
            extra_keys_str = ", ".join(key for key in extra_keys)
            warnings.warn(
                message=(
                    f"Found unexpected fields in configuration files for {self.name}. "
                    "These fields will be ignored. "
                    f"Fields:\n{extra_keys_str}\n(config dir: {self.directory})"
                )
            )

        return {k: v for k, v in config.items() if k in permitted_keys}

    # loads a list of config files, returning only those that exist
    def load_files(self, filenames: list[str]) -> list[DagsterConfigDict]:
        configs = [
            load_config(os.path.join(self.directory, config_name))
            for config_name in filenames
        ]
        if not any(configs):
            expected_files_str = ', '.join(filenames)
            raise ValueError(
                f"No configuration files detected for {self.name}! "
                f"Expected at least one of these files in {self.directory}:\n{expected_files_str}"
            )

        return [config for config in configs if config is not None]

    # loads the global and mode-specific config for the given mode,
    # and verifies that the loaded config is valid.
    def load_for_mode(self, mode: str) -> DagsterConfigDict:
        configs = self.load_files(['global.yaml', f"{mode}.yaml"])

        loaded_config: DagsterConfigDict = {}

        for config in configs:
            loaded_config = {
                **loaded_config,
                **config,
            }

        return self.validated_config(loaded_config)
