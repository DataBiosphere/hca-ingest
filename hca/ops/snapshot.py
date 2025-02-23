import time
from typing import List

from dagster import op, Out, In, Output


@op
def notify_snapshot_start():
    time.sleep(4)
    pass


@op
def submit_snapshot():
    time.sleep(4)
    pass


@op(ins={"snapshot_id": In(str)}, out={"job_result": Out(str)})
def retrieve_snapshot_info(snapshot_id: str):
    time.sleep(4)
    pass


@op(
    ins={"snapshot_id": In(str), "stewards": In(List[str])},
    out={"job_result": Out(str)},
)
def assign_steward(snapshot_id: str, stewards: List[str]):
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


@op(out={"is_public": Out(bool)}, config_schema={"is_public": bool})
def make_snapshot_public(is_public: bool = False):
    time.sleep(4)
    return {"is_public": is_public}


@op(ins={"snapshot_id": In(str)}, out={"job_result": Out(str)})
def wait_for_snapshot_completion(snapshot_id: str):
    time.sleep(4)
    return Output(f"Job completed for {snapshot_id}")


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
