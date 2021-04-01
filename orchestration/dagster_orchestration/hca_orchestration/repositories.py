from typing import Union

from dagster import PipelineDefinition, repository, SensorDefinition

from hca_orchestration.sensors import postvalidate_on_import_complete
from hca_orchestration.pipelines import load_hca_data, validate_egress


@repository
def hca_orchestrationtype() -> list[Union[PipelineDefinition, SensorDefinition]]:
    return [postvalidate_on_import_complete, load_hca_data, validate_egress]
