from datetime import datetime
import os
from typing import Iterator, Union

from dateutil.tz import tzlocal

from dagster import RunRequest, sensor, SensorExecutionContext, SkipReason, SensorDefinition

from dagster_utils.contrib.argo_workflows import ArgoArchivedWorkflowsClient, ExtendedArgoWorkflow
from dagster_utils.contrib.google import default_google_access_token


# boundary before which we don't care about any workflows in argo
ARGO_EPOCH: datetime = datetime(2021, 3, 15, tzinfo=tzlocal())


class ArgoHcaImportCompletionSensor(ArgoArchivedWorkflowsClient):
    def successful_hca_import_workflows(self) -> list[ExtendedArgoWorkflow]:
        return [
            ExtendedArgoWorkflow(workflow, argo_url=self.argo_url, access_token=self.access_token)
            for workflow
            in self.list_archived_workflows()
            if workflow.metadata.name.startswith("import-hca-total")
            and workflow.status.phase == 'Succeeded'
            and workflow.status.finished_at > ARGO_EPOCH
        ]

    def generate_run_request(self, workflow: ExtendedArgoWorkflow) -> RunRequest:
        inflated_workflow = workflow.inflate()

        dataset_name = inflated_workflow.params_dict()['data-repo-name'].removeprefix("datarepo_")
        argo_workflow_id = inflated_workflow.metadata.uid

        return RunRequest(
            run_key=inflated_workflow.metadata.name,
            run_config={
                "resources": {
                    "hca_dataset_operation_config": {
                        "config": {
                            "dataset_name": dataset_name
                        }
                    }
                },
                "solids": {
                    "notify_slack_of_egress_validation_results": {
                        "config": {
                            "argo_workflow_id": argo_workflow_id
                        }
                    }
                }
            }
        )


def build_post_import_sensor(env: str) -> SensorDefinition:
    """
    Builds a post-validation sensor, responsible for determining the source argo workflow and
    kicking off our post-import validation egress pipeline.
    :param env: Environment in which this sensor runs, corresponds to the "mode" as defined in the
    validate_egress module.
    """
    def _validate_on_import_complete(_: SensorExecutionContext) -> Union[Iterator[RunRequest], SkipReason]:
        sensor = ArgoHcaImportCompletionSensor(
            argo_url=os.environ["HCA_ARGO_URL"],
            access_token=default_google_access_token())

        workflows = sensor.successful_hca_import_workflows()

        if any(workflows):
            for workflow in workflows:
                yield sensor.generate_run_request(workflow)
        else:
            return SkipReason("No succeeded import-hca-total workflows returned by Argo.")

    return SensorDefinition(
        name="post_import_validate_egress",
        pipeline_name="validate_egress",
        mode=env,
        evaluation_fn=_validate_on_import_complete
    )
