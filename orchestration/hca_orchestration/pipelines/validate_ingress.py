from dagster import failure_hook, HookContext, graph, resource, InitResourceContext

from hca_manage.validation import HcaValidator
from hca_orchestration.solids.validate_ingress import pre_flight_validate, notify_slack_of_succesful_ingress_validation


@resource
def staging_area_validator(init_context: InitResourceContext) -> HcaValidator:
    return HcaValidator()


@failure_hook(
    required_resource_keys={'slack', 'dagit_config'}
)
def validation_failed_notification(context: HookContext) -> None:
    lines = ("Validation Ingress Failed",
             "Dagit link: " + f"<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>"
             )
    slack_msg_text = "\n".join(lines)
    context.resources.slack.send_message(text=slack_msg_text)


@graph
def validate_ingress_graph() -> None:
    notify_slack_of_succesful_ingress_validation(pre_flight_validate())
