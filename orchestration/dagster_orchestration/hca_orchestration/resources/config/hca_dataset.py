from dataclasses import dataclass

from dagster import InitResourceContext, resource, String, Int


@dataclass
class TargetHcaDataset:
    """
    Represents the target HCA dataset in Jade that will receive data
    during a pipeline run
    """
    dataset_name: str
    dataset_id: str
    project_id: str
    billing_profile_id: str

    def fully_qualified_jade_dataset_name(self) -> str:
        return f"datarepo_{self.dataset_name}"


@resource({
    "dataset_name": String,
    "dataset_id": String,
    "project_id": String,
    "billing_profile_id": String,
})
def target_hca_dataset(init_context: InitResourceContext) -> TargetHcaDataset:
    return TargetHcaDataset(**init_context.resource_config)
