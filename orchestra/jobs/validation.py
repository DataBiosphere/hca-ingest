from dagster import job

from orchestra.ops.validation import (
    pre_flight_validation,
    check_validation_status,
    notify_validation_success,
    notify_validation_failure,
    validate_ingress,
)


@job
def pre_flight_validation_job():
    validation_result = pre_flight_validation()
    validation_status = check_validation_status()

    if validation_status:
        notify_validation_success()
    else:
        notify_validation_failure()


@job
def validate_ingress_job():
    validation_result = validate_ingress()
    validation_status = check_validation_status()

    if validation_status:
        notify_validation_success()
    else:
        notify_validation_failure()
