from dagster import resource


class ConsoleSlackClient:
    def __init__(self, context):
        self.context = context

    def chat_postMessage(self, channel, text):
        self.context.log.info(f"[SLACK] {channel}: {text}")


@resource
def console_slack_client(init_context):
    return ConsoleSlackClient(init_context)
