from typing import ClassVar

from dagster import ConfigurableResource
from google.cloud import storage, bigquery

from hca.contrib.bigquery_client import bigquery_client, local_bigquery_client
from hca.contrib.storage_client import (
    google_storage_client,
    local_google_storage_client,
)


class GCPStorageResource(ConfigurableResource):
    gcs_client: ClassVar[storage.Client] = google_storage_client


class BigQueryResource(ConfigurableResource):
    bq_client: ClassVar[bigquery.Client] = bigquery_client


class BeamRunnerResource(ConfigurableResource):
    runner: ClassVar = None


class LocalGCPStorageResource(ConfigurableResource):
    gcs_client: ClassVar[storage.Client] = local_google_storage_client


class LocalBigQueryResource(ConfigurableResource):
    bq_client: ClassVar[bigquery.Client] = local_bigquery_client


class LocalBeamRunnerResource(ConfigurableResource):
    runner: ClassVar = None
