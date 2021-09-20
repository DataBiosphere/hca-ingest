from dagster import solid, String, Failure
from dagster.core.execution.context.compute import AbstractComputeExecutionContext


@solid(
    required_resource_keys={'staging_area_validator'},
    config_schema={
        "staging_area": String
    }
)
def pre_flight_validate(context: AbstractComputeExecutionContext) -> str:
    """
    Runs the external validation code on the provided staging area.
    """
    staging_area = context.solid_config["staging_area"]
    exit_code = context.resources.staging_area_validator.validate_staging_area(path=staging_area, ignore_inputs=True)
    if exit_code:
        raise Failure(f"Staging area {staging_area} is invalid")

    return staging_area


@solid(
    required_resource_keys={'slack'}
)
def notify_slack_of_succesful_ingress_validation(
    context: AbstractComputeExecutionContext,
    staging_area: str
) -> str:
    message_lines = [
        f"{staging_area} has passed pre-validation.",
    ]
    message = "\n".join(message_lines)

    context.resources.slack.send_message(message)

    return message
