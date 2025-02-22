from typing import ClassVar

from dagster import ConfigurableResource
from slack_sdk import WebClient

from orchestra.contrib.slack_client import slack_client, local_slack_client


class SlackResource(ConfigurableResource):
    slack_client: ClassVar[WebClient] = slack_client


class LocalSlackResource(ConfigurableResource):
    slack_client: ClassVar[WebClient] = local_slack_client
