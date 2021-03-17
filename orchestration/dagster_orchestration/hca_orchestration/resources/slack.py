import os

from dagster import configured, resource
from dagster.core.execution.context.init import InitResourceContext
from dagster_slack import slack_resource


class ConsoleSlackClient:
    def __init__(self, context: InitResourceContext):
        self.context = context

    def chat_postMessage(self, channel, text):
        self.context.log.info(f"[SLACK] {channel}: {text}")


@resource
def console_slack_client(init_context: InitResourceContext):
    return ConsoleSlackClient(init_context)


@configured(slack_resource)
def live_slack_client(_config):
    return {
        "token": os.environ.get("SLACK_TOKEN"),
    }
