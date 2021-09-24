from dataclasses import dataclass


@dataclass
class TdrDataset:
    """
    Represents the target HCA dataset in Jade that will receive data
    during a pipeline run
    """
    dataset_name: str
    dataset_id: str
    project_id: str
    billing_profile_id: str
    bq_location: str

    def fully_qualified_jade_dataset_name(self) -> str:
        return f"datarepo_{self.dataset_name}"
