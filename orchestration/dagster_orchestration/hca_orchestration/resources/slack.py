from dataclasses import dataclass

import slack
from dagster import DagsterLogManager, resource, String, StringSource
from dagster.core.execution.context.init import InitResourceContext


@dataclass
class ConsoleSlackClient:
    logger: DagsterLogManager

    def send_message(self, text: str) -> None:
        self.logger.info(f"[SLACK] {text}")


@resource
def console_slack_client(init_context: InitResourceContext) -> ConsoleSlackClient:
    return ConsoleSlackClient(init_context.log)


@dataclass
class LiveSlackClient:
    client: slack.WebClient
    channel: str

    def send_message(self, text: str) -> None:
        self.client.chat_postMessage(channel=self.channel, text=text)


@resource({
    'channel': String,
    'token': StringSource,
})
def live_slack_client(init_context: InitResourceContext) -> LiveSlackClient:
    return LiveSlackClient(
        slack.WebClient(init_context.resource_config['token']),
        init_context.resource_config['channel'],
    )
