import logging
import os
from typing import Any, Optional

from dagster import (
    PipelineRun,
    PipelineRunStatus,
    RunStatusSensorContext,
    file_relative_path,
    pipeline_failure_sensor,
    run_status_sensor,
)
from dagster.utils import load_yaml_from_globs
from dagster_slack import make_slack_on_pipeline_failure_sensor
from slack_sdk import WebClient


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
