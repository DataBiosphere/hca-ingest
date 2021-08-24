import os
from typing import Union

from dagster import PipelineDefinition, repository, SensorDefinition

from hca_orchestration.config.dev_refresh.dev_refresh import dev_refresh_partition_set
from hca_orchestration.pipelines import copy_project, cut_snapshot, load_hca, validate_egress
from hca_orchestration.sensors import build_post_import_sensor


@repository
def hca_orchestrationtype() -> list[Union[PipelineDefinition, SensorDefinition]]:
    defs = [
        cut_snapshot,
        load_hca,
        validate_egress,
        build_post_import_sensor(os.environ.get("ENV", "test")),
        copy_project,
        dev_refresh_partition_set()
    ]
    return defs
