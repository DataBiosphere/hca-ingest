from dagster import job

from orchestra.ops.snapshot import (
    notify_snapshot_start,
    submit_snapshot,
    wait_for_snapshot_completion,
    retrieve_snapshot_info,
    assign_steward,
    notify_snapshot_done,
    notify_snapshot_failure,
    get_snapshot_from_project,
    make_snapshot_public,
)


@job
def execute_cut_snapshot():
    notify_snapshot_start()
    snapshot_id = submit_snapshot()
    job_result = wait_for_snapshot_completion(snapshot_id)
    snapshot_info = retrieve_snapshot_info(job_result)
    assign_steward(snapshot_info)
    notify_snapshot_done()


@job
def execute_legacy_cut_snapshot():
    notify_snapshot_start()
    snapshot_id = submit_snapshot()
    job_result = wait_for_snapshot_completion(snapshot_id)
    snapshot_info = retrieve_snapshot_info(job_result)
    assign_steward(snapshot_info)
    notify_snapshot_done()


@job
def execute_make_snapshot_public():
    notify_snapshot_start()
    snapshot_id = get_snapshot_from_project()
    make_snapshot_public(snapshot_id)
    notify_snapshot_done()


@job
def execute_snapshot_failure_notification():
    notify_snapshot_failure()


@job
def execute_full_snapshot_workflow():
    notify_snapshot_start()
    snapshot_id = submit_snapshot()
    job_result = wait_for_snapshot_completion(snapshot_id)
    snapshot_info = retrieve_snapshot_info(job_result)
    assign_steward(snapshot_info)
    notify_snapshot_done()
    snapshot_id = get_snapshot_from_project()
    make_snapshot_public(snapshot_id)
    notify_snapshot_done()
