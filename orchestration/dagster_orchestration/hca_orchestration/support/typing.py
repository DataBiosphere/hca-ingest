"""
Complex type signatures that appear multiple times throughout the code
base can live here, for easy reference and descriptive naming.
"""

from dagster import InputDefinition
from dagster.config import ConfigType as DagsterConfigType

from typing import Literal, TypedDict, Union


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

DagsterSolidConfigSchema = dict[str, DagsterConfigType]


class DagsterSolidConfig(TypedDict, total=False):
    required_resource_keys: set[str]
    input_defs: list[InputDefinition]
    config_schema: DagsterSolidConfigSchema
