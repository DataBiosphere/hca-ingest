from dagster import repository

from .sensors import postvalidate_on_import_complete
from .pipelines import stage_data, validate_egress


@repository
def hca_orchestrationtype():
    return [postvalidate_on_import_complete, stage_data, validate_egress]
