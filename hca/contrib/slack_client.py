from dataclasses import dataclass
from typing import Callable, Optional

from dagster import ConfigurableResource, InitResourceContext, DagsterLogManager
from slack_sdk import WebClient

DagsterHookFunction = Callable[[InitResourceContext], None]
SlackMessageGenerator = Callable[[InitResourceContext], str]


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


