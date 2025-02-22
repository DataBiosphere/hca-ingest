import os

from orchestra.resources.gcp import GCPStorageResource, BeamRunnerResource, LocalGCPStorageResource, BigQueryResource, \
    LocalBigQueryResource, LocalBeamRunnerResource
from orchestra.resources.slack import SlackResource, LocalSlackResource
from orchestra.resources.terra import TerraDataRepoResource, LocalTerraDataRepoResource

HCA_ENV = os.getenv("ENVIRONMENT", "development").lower().strip("")

if HCA_ENV == "production":
    resources = {
        "gcs": GCPStorageResource(),
        "bigquery": BigQueryResource(),
        "tdr": TerraDataRepoResource(),
        "slack": SlackResource(),
        "beam_runner": BeamRunnerResource(),
    }
elif HCA_ENV == "local":
    resources = {
        "gcs": LocalGCPStorageResource(),
        "bigquery": LocalBigQueryResource(),
        "tdr": LocalTerraDataRepoResource(),
        "slack": LocalSlackResource(),
        "beam_runner": LocalBeamRunnerResource(),
    }
elif HCA_ENV == "development":
    resources = {
        "gcs": GCPStorageResource(),
        "bigquery": BigQueryResource(),
        "tdr": TerraDataRepoResource(),
        "slack": SlackResource(),
        "beam_runner": BeamRunnerResource(),
    }
else:
    raise ValueError(f"Unknown environment: {HCA_ENV}")
