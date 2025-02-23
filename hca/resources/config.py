import os

from hca.resources.gcp import (
    GCPStorageResource,
    BeamRunnerResource,
    LocalGCPStorageResource,
    BigQueryResource,
    LocalBigQueryResource,
    LocalBeamRunnerResource,
)
from hca.resources.slack import SlackResource, LocalSlackResource
from hca.resources.terra import TerraDataRepoResource, LocalTerraDataRepoResource
from hca.resources.validation import HcaValidatorResource, LocalHcaValidatorResource

HCA_ENV = os.getenv("ENVIRONMENT", "local").lower().strip("")

if HCA_ENV == "production":
    resources = {
        "gcs": GCPStorageResource(),
        "bigquery": BigQueryResource(),
        "tdr": TerraDataRepoResource(),
        "slack": SlackResource(token="your-slack-token", channel="#production-channel"),
        "beam_runner": BeamRunnerResource(),
        "hca_validator": HcaValidatorResource()
    }
elif HCA_ENV == "local":
    resources = {
        "gcs": LocalGCPStorageResource(),
        "bigquery": LocalBigQueryResource(),
        "tdr": LocalTerraDataRepoResource(),
        "slack": LocalSlackResource(),
        "beam_runner": LocalBeamRunnerResource(),
        "hca_validator": LocalHcaValidatorResource()
    }
elif HCA_ENV == "development":
    resources = {
        "gcs": GCPStorageResource(),
        "bigquery": BigQueryResource(),
        "tdr": TerraDataRepoResource(),
        "slack": SlackResource(token="your-slack-token", channel="#development-channel"),
        "beam_runner": BeamRunnerResource(),
        "hca_validator": HcaValidatorResource()
    }
else:
    raise ValueError(f"Unknown environment: {HCA_ENV}")
