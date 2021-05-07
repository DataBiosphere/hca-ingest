"""
Complex type signatures that appear multiple times throughout the code
base can live here, for easy reference and descriptive naming.
"""

from dagster import InputDefinition, usable_as_dagster_type
from dagster.config import ConfigType as DagsterConfigType

from typing import Literal, TypedDict, Union

# dict of config settings for a given instance of a dagster object
DagsterConfigDict = dict[
    str,
    Union[
        dict[
            Literal['env'],
            str
        ],
        str,
        int,
        float,
        bool
    ]
]

# dict representing how a solid can be configured
DagsterSolidConfigSchema = dict[str, DagsterConfigType]


class DagsterSolidConfig(TypedDict, total=False):
    required_resource_keys: set[str]
    input_defs: list[InputDefinition]
    config_schema: DagsterSolidConfigSchema


class HcaScratchDatasetName(str):
    pass
