import os

from dagster import sensor, RunRequest, SkipReason

from argo.workflows.client import ApiClient as ArgoApiClient,\
                                  ArchivedWorkflowServiceApi,\
                                  Configuration as ArgoConfiguration
from .resources.base import default_google_access_token


# this isn't defined as a resource because resources aren't available to sensors
def generate_argo_workflows_client(host_url):
    return ArchivedWorkflowServiceApi(
        api_client=ArgoApiClient(
            configuration=ArgoConfiguration(host=host_url),
            header_name="Authorization",
            header_value=f"Bearer {default_google_access_token()}"))


# TODO use execution context to avoid re-scanning old workflows
@sensor(pipeline_name="validate_egress")
def postvalidate_on_import_complete(_):
    client = generate_argo_workflows_client()

    workflows = [
        workflow
        for workflow
        in client.list_archived_workflows()
        if workflow.metadata.name.startswith("import-hca-total")
        and workflow.status.phase == 'Succeeded'
    ]

    if any(workflows):
        for workflow in workflows:
            # the list-workflows endpoint doesn't include parameter metadata, so we need to make a separate get
            # request to inflate the workflow once we know we want to do something with it.
            inflated_workflow = client.get_archived_workflow(workflow.metadata.uid)
            workflow_params = {
                param.name: param.value
                for param in inflated_workflow.spec.arguments.parameters
            }

            yield RunRequest(
                run_key=inflated_workflow.metadata.name,
                run_config={
                    "solids": {
                        "post_import_validate": {
                            "config": {
                                "gcp_env": os.environ.get("HCA_GCP_ENV"),
                                "google_project_name": os.environ.get("HCA_GOOGLE_PROJECT"),
                                "dataset_name": workflow_params['data-repo-name'].removeprefix("datarepo_"),
                            }
                        }
                    }
                }
            )
    else:
        return SkipReason("No succeeded import-hca-total workflows returned by Argo.")
