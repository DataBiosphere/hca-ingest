from dataclasses import dataclass

import slack
from dagster import configured, DagsterLogManager, resource, String, StringSource
from dagster.core.execution.context.init import InitResourceContext

from hca_orchestration.support.typing import DagsterConfigDict


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
def untokened_live_slack_client(init_context: InitResourceContext) -> LiveSlackClient:
    return LiveSlackClient(
        slack.WebClient(init_context.resource_config['token']),
        init_context.resource_config['channel'],
    )


# this can't be fully configured using preconfigure_for_mode, since SLACK_TOKEN is a secret
@configured(untokened_live_slack_client, {'channel': String})
def live_slack_client(config: DagsterConfigDict) -> DagsterConfigDict:
    return {
        'token': {'env': 'SLACK_TOKEN'},
        **config,
    }


live_slack_client.__name__ = 'live_slack_client'
