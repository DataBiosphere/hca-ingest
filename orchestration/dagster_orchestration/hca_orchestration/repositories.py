from dagster import repository

from hca_orchestration.sensors import postvalidate_on_import_complete
from hca_orchestration.pipelines import stage_data, validate_egress


@repository
def hca_orchestrationtype():
    return [postvalidate_on_import_complete, stage_data, validate_egress]
