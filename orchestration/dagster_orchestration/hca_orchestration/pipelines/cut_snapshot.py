from dagster import ModeDefinition, pipeline

from hca_orchestration.solids.create_snapshot import create_snapshot, make_snapshot_public
from hca_orchestration.resources import jade_data_repo_client, noop_data_repo_client
from hca_orchestration.resources.sam import prod_sam_client, noop_sam_client


prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "data_repo_client": jade_data_repo_client,
        "sam_client": prod_sam_client,
    }
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "data_repo_client": jade_data_repo_client,
        # we don't want to actually hit sam and make a snapshot public
        # unless we're running in prod
        "sam_client": noop_sam_client,
    }
)

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "data_repo_client": noop_data_repo_client,
        "sam_client": noop_sam_client,
    }
)


@pipeline(
    mode_defs=[prod_mode, local_mode, test_mode]
)
def cut_snapshot() -> None:
    make_snapshot_public(create_snapshot())
