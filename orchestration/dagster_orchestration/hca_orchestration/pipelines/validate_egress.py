from dagster import ModeDefinition, pipeline

from hca_orchestration.resources.base import jade_data_repo_client
from hca_orchestration.resources.test import noop_data_repo_client
from hca_orchestration.solids import post_import_validate


prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "data_repo_client": jade_data_repo_client
    }
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "data_repo_client": jade_data_repo_client
    }
)

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "data_repo_client": noop_data_repo_client
    }
)


@pipeline(
    mode_defs=[prod_mode, local_mode, test_mode]
)
def validate_egress():
    post_import_validate()
