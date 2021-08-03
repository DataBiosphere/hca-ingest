from typing import Union

from dagster import PipelineDefinition, repository, SensorDefinition

from hca_orchestration.sensors import build_post_import_sensor
from hca_orchestration.pipelines import copy_project, cut_snapshot, load_hca, validate_egress

import os


@repository
def hca_orchestrationtype() -> list[Union[PipelineDefinition, SensorDefinition]]:
    defs = [
        cut_snapshot,
        load_hca,
        validate_egress,
        build_post_import_sensor(os.environ.get("ENV", "test")),
        copy_project
    ]
    return defs
