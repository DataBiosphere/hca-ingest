from __future__ import annotations  # this lets us annotate functions in class C that return an instance of C

from typing import Any, Dict, Generator, Optional
from typing_extensions import Protocol

from argo.workflows.client import ApiClient as ArgoApiClient,\
    ArchivedWorkflowServiceApi,\
    Configuration as ArgoConfiguration
from argo.workflows.client.models import V1alpha1Workflow, V1alpha1WorkflowList


def generate_argo_archived_workflows_client(host_url: str, access_token: str) -> ArchivedWorkflowServiceApi:
    return ArchivedWorkflowServiceApi(
        api_client=ArgoApiClient(
            configuration=ArgoConfiguration(host=host_url),
            header_name="Authorization",
            header_value=f"Bearer {access_token}"))


class ArgoFetchListOperation(Protocol):
    def __call__(
        self,
        *args: Any,
        list_option_continue: Optional[str] = None,
        **kwargs: Any
    ) -> V1alpha1WorkflowList: ...


class ArgoArchivedWorkflowsClient:
    def __init__(self, argo_url: str, access_token: str):
        self.argo_url = argo_url
        self.access_token = access_token
        self._client = None

    def client(self) -> ArchivedWorkflowServiceApi:
        if not self._client:
            self._client = generate_argo_archived_workflows_client(self.argo_url, self.access_token)

        return self._client

    def list_archived_workflows(self) -> Generator[V1alpha1Workflow, None, None]:
        return self._pull_paginated_results(self.client().list_archived_workflows)

    def get_archived_workflow(self, uid: str) -> V1alpha1Workflow:
        return self.client().get_archived_workflow(uid)

    def _pull_paginated_results(self, api_function: ArgoFetchListOperation) -> Generator[V1alpha1Workflow, None, None]:
        results = api_function()

        for result in results.items:
            yield result

        while results.metadata._continue:
            results = api_function(list_options_continue=results.metadata._continue)

            for result in results.items:
                yield result


class ExtendedArgoWorkflow:
    def __init__(self, workflow: V1alpha1Workflow, argo_url: str, access_token: str):
        self._workflow = workflow
        self._inflated = False
        self.client = ArgoArchivedWorkflowsClient(argo_url, access_token)

    def inflate(self) -> ExtendedArgoWorkflow:
        if not self._inflated:
            self._workflow = self.client.get_archived_workflow(self.metadata.uid)
            self._inflated = True

        return self

    def params_dict(self) -> Dict[str, Any]:
        return {
            param.name: param.value
            for param in self.spec.arguments.parameters
        }

    # proxy pattern - any function calls not defined in this class are passed to the wrapped workflow object
    def __getattr__(self, name):
        return getattr(self._workflow, name)

    def __eq__(self, other):
        return isinstance(other, ExtendedArgoWorkflow) and self._workflow == other._workflow
