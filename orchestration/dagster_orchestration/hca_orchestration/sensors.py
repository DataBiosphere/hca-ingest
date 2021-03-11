from datetime import datetime
import os
from typing import List

from dateutil.tz import tzlocal

from dagster import sensor, RunRequest, SkipReason

from hca_orchestration.contrib.argo_workflows import ArgoArchivedWorkflowsClientMixin, ExtendedArgoWorkflow
from hca_orchestration.resources.base import default_google_access_token


# boundary before which we don't care about any workflows in argo
ARGO_EPOCH = datetime(2021, 3, 1, tzinfo=tzlocal())


class ArgoHcaImportCompletionSensor(ArgoArchivedWorkflowsClientMixin):
    def successful_hca_import_workflows(self) -> List[ExtendedArgoWorkflow]:
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

        return RunRequest(
            run_key=inflated_workflow.metadata.name,
            run_config={
                "solids": {
                    "post_import_validate": {
                        "config": {
                            "gcp_env": os.environ.get("HCA_GCP_ENV"),
                            "google_project_name": os.environ.get("HCA_GOOGLE_PROJECT"),
                            "dataset_name": inflated_workflow.params_dict()['data-repo-name'].removeprefix("datarepo_"),
                        }
                    }
                },
                # dagster requires config settings for all resources in the pipeline, even if they're unused,
                # so we pass in dummy values here
                "resources": {
                    "beam_runner": {
                        "config": {
                            "project": '',
                            "temp_location": '',
                            "subnet_name": '',
                            "service_account": '',
                            "image_name": '',
                            "image_version": '',
                            "namespace": ''
                        }
                    },
                    "data_repo_client": {
                        "config": {
                            "api_url": ''
                        }
                    }
                }
            }
        )


# TODO use execution context to avoid re-scanning old workflows
@sensor(pipeline_name="validate_egress", mode="prod")
def postvalidate_on_import_complete(_):
    sensor = ArgoHcaImportCompletionSensor(argo_url=os.environ.get("HCA_ARGO_URL"), access_token=default_google_access_token())

    workflows = sensor.successful_hca_import_workflows()

    if any(workflows):
        for workflow in workflows:
            yield sensor.generate_run_request(workflow)
    else:
        return SkipReason("No succeeded import-hca-total workflows returned by Argo.")
