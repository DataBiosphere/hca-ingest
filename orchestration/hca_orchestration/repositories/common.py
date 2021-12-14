import logging
import os
from typing import Any, Optional

from dagster import PipelineDefinition, in_process_executor, run_status_sensor, PipelineRunStatus, \
    RunStatusSensorContext, pipeline_failure_sensor, PipelineRun, file_relative_path
from dagster.utils import load_yaml_from_globs
from dagster_gcp.gcs import gcs_pickle_io_manager
from dagster_slack import make_slack_on_pipeline_failure_sensor
from dagster_utils.resources.bigquery import bigquery_client
from dagster_utils.resources.data_repo.jade_data_repo import jade_data_repo_client
from dagster_utils.resources.google_storage import google_storage_client
from slack_sdk import WebClient

from hca_orchestration.config import preconfigure_resource_for_mode
from hca_orchestration.pipelines import copy_project
from hca_orchestration.resources.config.scratch import scratch_config
from hca_orchestration.resources import bigquery_service, load_tag
from hca_orchestration.resources.hca_project_config import hca_project_copying_config, hca_project_id
from hca_orchestration.resources.config.datasets import find_or_create_project_dataset
from hca_orchestration.resources.data_repo_service import data_repo_service
from hca_orchestration.resources.utils import run_start_time


def copy_project_to_new_dataset_job(src_env: str, target_env: str) -> PipelineDefinition:
    return copy_project.to_job(
        name=f"copy_project_from_{src_env}_to_{target_env}",
        description=f"Copies a project from {src_env} to {target_env}",
        resource_defs={
            "bigquery_client": bigquery_client,
            "data_repo_client": preconfigure_resource_for_mode(jade_data_repo_client, target_env),
            "gcs": google_storage_client,
            "scratch_config": scratch_config,
            "bigquery_service": bigquery_service,
            "hca_project_copying_config": hca_project_copying_config,
            "target_hca_dataset": find_or_create_project_dataset,
            "hca_project_id": hca_project_id,
            "load_tag": load_tag,
            "data_repo_service": data_repo_service,
            "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, src_env),
            "run_start_time": run_start_time
        },
        executor_def=in_process_executor
    )


def config_path(relative_path: str) -> str:
    path: str = file_relative_path(
        __file__, os.path.join("../config/", relative_path)
    )
    return path


def _slack_config() -> Any:
    return load_yaml_from_globs(config_path("resources/live_slack_client/global.yaml"))


def _slack_channel() -> str:
    return str(_slack_config()["channel"])


def _slack_token() -> Optional[str]:
    return os.getenv(_slack_config()["token"]["env"])


def build_pipeline_failure_sensor() -> pipeline_failure_sensor:
    slack_on_pipeline_failure = make_slack_on_pipeline_failure_sensor(
        channel=_slack_channel(),
        slack_token=_slack_token()
    )
    return slack_on_pipeline_failure


@run_status_sensor(pipeline_run_status=PipelineRunStatus.STARTED)
def slack_on_pipeline_start(context: RunStatusSensorContext) -> None:
    _slack_on_pipeline_status(context.pipeline_run, PipelineRunStatus.STARTED)


@run_status_sensor(pipeline_run_status=PipelineRunStatus.SUCCESS)
def slack_on_pipeline_success(context: RunStatusSensorContext) -> None:
    _slack_on_pipeline_status(context.pipeline_run, PipelineRunStatus.SUCCESS)


def _slack_on_pipeline_status(pipeline_run: PipelineRun, status: PipelineRunStatus) -> None:
    slack_token = _slack_token()
    channel = _slack_channel()

    if not slack_token:
        logging.info(f"No slack token set, will not notify pipeline {status.value}")
        return

    slack_client = WebClient(token=slack_token)
    lines = [
        f"HCA Pipeline entered state {status.value}",
        f"Name = {pipeline_run.pipeline_name}",
        f"Run ID = {pipeline_run.run_id}"
    ]
    slack_client.chat_postMessage(channel=channel, text="\n".join(lines))
