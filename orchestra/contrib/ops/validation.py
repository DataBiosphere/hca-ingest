import time
from dagster import op

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
