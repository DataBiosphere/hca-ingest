"""
Reusable config schemas common to multiple solids/resources can live here.
To add a config schema template to your solid, use it like so:

@solid(config_schema={**MY_SCHEMA_TEMPLATE, **OTHER_TEMPLATE, 'additional_field': String})

If multiple templates contain the same field, it'll use the value in whatever template is listed last,
of the templates that contain the field in question.
"""
from dagster import String, StringSource

from hca_orchestration.support.typing import DagsterSolidConfigSchema


HCA_MANAGE_SCHEMA: DagsterSolidConfigSchema = {
    "gcp_env": StringSource,
    "dataset_name": String,
    "google_project_name": StringSource,
}
