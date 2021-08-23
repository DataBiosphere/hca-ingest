from dagster import solid, String
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

from hca_manage.common import ProblemCount

from hca_manage.validation import run as validate

@solid(
    required_resource_keys={'data_repo_client'},
    config_schema={
        "staging_area": String
    }
)
def pre_flight_validate(context: AbstractComputeExecutionContext) -> ProblemCount:
    """
    Runs the external validation code on the provided staging area.
    """
    staging_area = context.solid_config["dataset_name"]
    validate(path=staging_area)

@solid(
    required_resource_keys={'slack'},
    config_schema={
        "staging_area": String
    }
)
def notify_slack_of_ingress_validation_results(
    context: AbstractComputeExecutionContext,
    validation_results: ProblemCount,
) -> str:
    staging_area = context.solids.pre_flight_validate["dataset_name"]

    if validation_results.has_problems():
        message_lines = [
            f"Problems identified in pre-flight validation for staging area {staging_area}:",
            "Issues found: " + str(validation_results)
        ]
    else:
        message_lines = [
            f"{staging_area} has passed pre-validation.",
        ]
    message = "\n".join(message_lines)

    context.resources.slack.send_message(message)

    return message
