from dagster import (
    HookContext,
    InitResourceContext,
    Partition,
    failure_hook,
    graph,
    resource,
)
from dagster_utils.typing import DagsterObjectConfigSchema
from hca_manage.validation import HcaValidator
from hca_orchestration.solids.validate_ingress import (
    notify_slack_of_successful_ingress_validation,
    pre_flight_validate,
)


def run_config_for_validation_ingress_partition(
    partition: Partition,
) -> DagsterObjectConfigSchema:
    return {
        "solids": {
            "pre_flight_validate": {
                "config": {
                    "staging_area": partition.value,
                    "total_retries": partition.value,
                }
            }
        },
    }


@resource
def staging_area_validator(init_context: InitResourceContext) -> HcaValidator:
    return HcaValidator()


@failure_hook(required_resource_keys={"slack", "dagit_config"})
def validation_failed_notification(context: HookContext) -> None:
    lines = (
        "Validation Ingress Failed",
        "Dagit link: "
        + f"<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>",
    )
    slack_msg_text = "\n".join(lines)
    context.resources.slack.send_message(text=slack_msg_text)


@graph
def validate_ingress_graph() -> None:
    # pylint: disable=no-value-for-parameter
    notify_slack_of_successful_ingress_validation(pre_flight_validate())
