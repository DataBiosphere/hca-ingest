import time

from dagster import op, In


@op(ins={"message": In(str)})
def slack_notification(message: str, channel: str = "#hca-ingest"):
    time.sleep(4)
    pass


@op(ins={"message": In(str)})
def email_notification(message: str):
    time.sleep(4)
    pass
