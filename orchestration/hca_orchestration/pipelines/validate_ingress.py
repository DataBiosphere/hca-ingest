from dagster import failure_hook, HookContext, graph, resource, InitResourceContext

from hca_manage.validation import HcaValidator
from hca_orchestration.contrib.slack import key_value_slack_blocks
from hca_orchestration.solids.validate_ingress import pre_flight_validate, notify_slack_of_succesful_ingress_validation


@resource
def staging_area_validator(init_context: InitResourceContext) -> HcaValidator:
    return HcaValidator()


@failure_hook(
    required_resource_keys={'slack', 'dagit_config'}
)
def validation_failed_notification(context: HookContext) -> None:
    kvs = {
        "Dagit link": f'<{context.resources.dagit_config.run_url(context.run_id)}|View in Dagit>'
    }
    context.resources.slack.send_message(blocks=key_value_slack_blocks("Validation Ingress Failed", key_values=kvs))


@graph
def validate_ingress_graph() -> None:
    notify_slack_of_succesful_ingress_validation(pre_flight_validate())
