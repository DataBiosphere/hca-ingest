from dagster import job

from hca.ops.validation import (
    check_validation_status,
    notify_validation_success,
    notify_validation_failure,
    validate_ingress, validate_staging_area,
)
from hca.utils.notifications import notify_failure, notify_success


@job(hooks={notify_failure, notify_success})
def pre_ingest_validation():
    validate_staging_area()


@job
def post_ingest_validation():
    validation_result = validate_ingress()
    validation_status = check_validation_status()

    if validation_status:
        notify_validation_success()
    else:
        notify_validation_failure()
