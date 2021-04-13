# Complex type signatures that appear multiple times throughout the code base can live here,
# for easy reference and descriptive naming.

from typing import Literal, Union, TypedDict


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


class MyConfig(TypedDict):
    config: DagsterConfigDict
