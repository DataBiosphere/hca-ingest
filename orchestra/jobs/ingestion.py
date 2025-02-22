import time

from dagster import job, op, In, Out, Output

from orchestra.ops.ingestion import (
    clear_staging_directory,
    preprocess_metadata,
    create_staging_dataset,
    import_data_files,
    fanout_file_metadata,
    fanout_non_file_metadata,
)
from orchestra.ops.notifications import slack_notification
from orchestra.ops.validation import check_validation_status


@op(ins={"validation_status": In(bool)}, out={"message": Out(str)})
def get_slack_message(validation_status):
    time.sleep(4)
    if validation_status:
        return Output("Data ingestion validation success")
    return Output("Data ingestion validation failed")


@job
def execute_data_ingestion():
    clear_staging_directory()
    preprocess_metadata()
    create_staging_dataset()
    import_data_files()
    fanout_file_metadata()
    fanout_non_file_metadata()

    validation_status = check_validation_status()
    message = get_slack_message(validation_status)
    slack_notification(message)
