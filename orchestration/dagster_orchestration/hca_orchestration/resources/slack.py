import os

from dagster import configured, resource, String
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


@configured(slack_resource, {"token": String})
def live_slack_client(config):
    return {
        "channel": os.environ.get("SLACK_NOTIFICATIONS_CHANNEL"),
        **config,
    }
