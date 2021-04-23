from datetime import datetime
from dateutil.tz import tzlocal
from typing import Any

from argo.workflows.client.models import V1alpha1Workflow, V1ObjectMeta, V1alpha1WorkflowSpec, V1alpha1Arguments, V1alpha1Parameter, \
    V1alpha1WorkflowStatus

from hca_orchestration.contrib.argo_workflows import ExtendedArgoWorkflow

# the argo workflows api produces this abominable nested set of classes for each workflow,
# so we build one from simple params here to keep our tests lean


def mock_argo_workflow(name: str, uid: str, status: str, finished_at: datetime = datetime.now(
        tz=tzlocal()), params: dict[str, Any] = {}) -> V1alpha1Workflow:
    return V1alpha1Workflow(
        metadata=V1ObjectMeta(
            name=name,
            uid=uid,
        ),
        spec=V1alpha1WorkflowSpec(
            arguments=V1alpha1Arguments(
                parameters=[
                    V1alpha1Parameter(name=k, value=v)
                    for k, v in params.items()
                ]
            )
        ),
        status=V1alpha1WorkflowStatus(
            phase=status,
            finished_at=finished_at,
        )
    )


def extend_workflow(workflow: V1alpha1Workflow) -> ExtendedArgoWorkflow:
    return ExtendedArgoWorkflow(workflow, argo_url='https://nonexistentsite.test', access_token='token')
