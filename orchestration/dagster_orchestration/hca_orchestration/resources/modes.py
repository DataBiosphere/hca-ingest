"""
Collection of Dagster modes that allow us to vary pipeline behavior between
environments.

(see https://docs.dagster.io/overview/modes-resources-presets/modes-resources for more details)
"""

from dagster import ModeDefinition
from hca_orchestration.resources.base import dataflow_beam_runner, local_beam_runner, google_storage_client, \
    jade_data_repo_client
from hca_orchestration.resources.test import test_beam_runner, local_storage_client, noop_data_repo_client

prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "beam_runner": dataflow_beam_runner,
        "storage_client": google_storage_client,
        "data_repo_client": jade_data_repo_client
    }
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "beam_runner": local_beam_runner,
        "storage_client": google_storage_client,
        "data_repo_client": jade_data_repo_client
    }
)

test_mode = ModeDefinition(
    name='test',
    resource_defs={
        "beam_runner": test_beam_runner,
        "storage_client": local_storage_client,
        "data_repo_client": noop_data_repo_client
    }
)
