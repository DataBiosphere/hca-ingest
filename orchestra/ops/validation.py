import time

from dagster import op, Out, Output


@op(out={"is_valid": Out(bool)})
def check_validation_status():
    time.sleep(4)
    return Output(True)


@op
def validate_ingress():
    time.sleep(4)
    pass


@op
def pre_flight_validation():
    time.sleep(4)
    pass


@op
def notify_validation_success():
    time.sleep(4)
    pass


@op
def notify_validation_failure():
    time.sleep(4)
    pass
