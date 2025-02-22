import time

from dagster import op


@op
def notify_snapshot_start():
    time.sleep(4)
    pass


@op
def submit_snapshot():
    time.sleep(4)
    pass


@op
def wait_for_snapshot_completion():
    time.sleep(4)
    pass


@op
def retrieve_snapshot_info():
    time.sleep(4)
    pass


@op
def assign_steward():
    time.sleep(4)
    pass


@op
def notify_snapshot_done():
    time.sleep(4)
    pass


@op
def notify_snapshot_failure():
    time.sleep(4)
    pass


@op
def get_snapshot_from_project():
    time.sleep(4)
    pass


@op
def make_snapshot_public():
    time.sleep(4)
    pass


@op
def submit_snapshot_job():
    time.sleep(4)
    pass


@op
def wait_for_snapshot_completion():
    time.sleep(4)
    pass


@op
def retrieve_snapshot_metadata():
    time.sleep(4)
    pass


@op
def add_snapshot_steward():
    time.sleep(4)
    pass


@op
def notify_snapshot_completion():
    time.sleep(4)
    pass


@op
def notify_snapshot_failure():
    time.sleep(4)
    pass
