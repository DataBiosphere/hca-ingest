from dagster import ModeDefinition, pipeline, failure_hook, HookContext, ResourceDefinition, graph, PipelineDefinition, \
    resource, InitResourceContext

from dagster_utils.resources.google_storage import google_storage_client, mock_storage_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client, noop_data_repo_client
from dagster_utils.resources.slack import console_slack_client, live_slack_client

from hca_manage.validation import HcaValidator
from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.contrib.slack import key_value_slack_blocks
from hca_orchestration.solids.validate_ingress import pre_flight_validate, notify_slack_of_succesful_ingress_validation

# prod_mode = ModeDefinition(
#     name="prod",
#     resource_defs={
#         "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "prod"),
#         "gcs": google_storage_client,
#         "slack": preconfigure_resource_for_mode(live_slack_client, "prod"),
#         "staging_area_validator": ResourceDefinition.mock_resource()
#     }
# )
#
# dev_mode = ModeDefinition(
#     name="dev",
#     resource_defs={
#         "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
#         "gcs": google_storage_client,
#         "slack": preconfigure_resource_for_mode(live_slack_client, "dev"),
#         "staging_area_validator": ResourceDefinition.mock_resource()
#     }
# )
#
# local_mode = ModeDefinition(
#     name="local",
#     resource_defs={
#         "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, "dev"),
#         "gcs": google_storage_client,
#         "slack": console_slack_client,
#         "staging_area_validator": ResourceDefinition.mock_resource()
#     }
# )
#
# test_mode = ModeDefinition(
#     name="test",
#     resource_defs={
#         "data_repo_client": noop_data_repo_client,
#         "gcs": mock_storage_client,
#         "slack": console_slack_client,
#         "staging_area_validator": ResourceDefinition.mock_resource()
#     }
# )
#


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
