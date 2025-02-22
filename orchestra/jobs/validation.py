from dagster import job, op

from orchestra.resources.gcp import GCPStorageResource
from orchestra.resources.slack import SlackResource


@op(required_resource_keys={"slack", "gcp_storage"})
def validate_ingress():
    pass


@job(resource_defs={"slack": SlackResource(), "gcp_storage": GCPStorageResource()})
def validate_ingress_job():
    validate_ingress()
