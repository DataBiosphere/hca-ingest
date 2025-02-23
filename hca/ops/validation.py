import time

from dagster import op, Out, Output, String, Failure, OpExecutionContext
from google.cloud.storage import Client

from hca.resources.validation import HcaValidatorResource


@op(out={"is_valid": Out(bool)})
def check_validation_status():
    time.sleep(4)
    return Output(True)


@op(
    required_resource_keys={"hca_validator", "gcs"},
    config_schema={"staging_area": String},
)
def validate_staging_area(context: OpExecutionContext) -> str:
    staging_area = context.op_config["staging_area"]
    gcs_client: Client = context.resources.gcs
    validator_resource: HcaValidatorResource = context.resources.hca_validator
    exit_code = validator_resource.validator.validate_staging_area(
        path=staging_area,
        ignore_inputs=True,
        client=gcs_client,
    )
    if exit_code:
        raise Failure(f"Validation failed for staging area: {staging_area}")

    return staging_area


@op
def validate_ingress():
    time.sleep(4)
    pass


@op
def pre_flight_validation():
    time.sleep(4)
    pass


@op
def notify_validation_success():
    time.sleep(4)
    pass


@op
def notify_validation_failure():
    time.sleep(4)
    pass
