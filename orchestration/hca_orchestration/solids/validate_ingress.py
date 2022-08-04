from typing import Any

from dagster import solid, String, Failure
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from google.cloud.storage import Client

from hca_manage.validation import HcaValidator


@solid(
    required_resource_keys={'staging_area_validator', 'gcs'},
    config_schema={
        "staging_area": String,
        "total_retries": int
    }
)
def pre_flight_validate(context: AbstractComputeExecutionContext) -> Any:
    """
    Runs the external validation code on the provided staging area.
    """
    staging_area = context.solid_config["staging_area"]
    total_retries = context.solid_config["total_retries"]
    gcs_client: Client = context.resources.gcs
    validator: HcaValidator = context.resources.staging_area_validator

    exit_code = validator.validate_staging_area(path=staging_area, ignore_inputs=True, client=gcs_client)
    if exit_code:
        raise Failure(f"Staging area {staging_area} is invalid")

    return staging_area, total_retries


@solid(
    required_resource_keys={'slack'}
)
def notify_slack_of_successful_ingress_validation(
    context: AbstractComputeExecutionContext,
    staging_area: str
) -> str:
    message_lines = [
        f"{staging_area} has passed pre-validation.",
    ]
    message = "\n".join(message_lines)

    context.resources.slack.send_message(message)

    return message
