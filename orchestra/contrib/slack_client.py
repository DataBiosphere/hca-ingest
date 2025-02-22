from dataclasses import dataclass
from typing import Callable, Optional

from dagster import (
    DagsterLogManager,
    HookContext,
    resource,
    String,
    StringSource,
    InitResourceContext,
)
from slack_sdk import WebClient

DagsterHookFunction = Callable[[HookContext], None]

SlackMessageGenerator = Callable[[HookContext], str]


@dataclass
class LocalSlackClient:
    logger: DagsterLogManager

    def send_message(
        self,
        text: Optional[str] = None,
        blocks: Optional[list[dict[str, object]]] = None,
    ) -> None:
        self.logger.info(f"[SLACK] {text} {blocks}")


@dataclass
class SlackClient:
    client: WebClient
    channel: str

    def send_message(
        self,
        text: Optional[str] = None,
        blocks: Optional[list[dict[str, object]]] = None,
    ) -> None:
        self.client.chat_postMessage(channel=self.channel, text=text, blocks=blocks)


@resource
def local_slack_client(init_context: InitResourceContext) -> LocalSlackClient:
    return LocalSlackClient(init_context.log)


@resource(
    {
        "channel": String,
        "token": StringSource,
    }
)
def slack_client(init_context: InitResourceContext) -> SlackClient:
    return SlackClient(
        WebClient(init_context.resource_config["token"]),
        init_context.resource_config["channel"],
    )
