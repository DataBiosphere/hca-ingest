from typing import Optional

from dagster import ConfigurableResource
from slack_sdk import WebClient


class SlackResource(ConfigurableResource):
    token: str
    channel: str

    def send_message(self, text: str, blocks: Optional[list[dict[str, object]]] = None):
        client = WebClient(token=self.token)
        client.chat_postMessage(channel=self.channel, text=text, blocks=blocks)


class LocalSlackResource(ConfigurableResource):

    def send_message(self, text: str, blocks: Optional[list[dict[str, object]]] = None):
        print(f"[SLACK] {text} {blocks}")
