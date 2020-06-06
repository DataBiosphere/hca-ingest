import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import polling
import os
import random
import sys

credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])

base_url = os.environ["API_URL"]
job_id = os.environ["JOB_ID"]
timeout = os.environ["TIMEOUT"]

authed_session = AuthorizedSession(credentials)


def check_job_status(job_id: str):
    response = authed_session.get(f"{base_url}/api/repository/v1/jobs/{job_id}")
    if response.ok:
        return response.json()["job_status"]
    else:
        raise HTTPError("Bad response, got code of: {}".format(response.status_code))


def is_done(job_id: str) -> bool:
    # if "running" then we want to keep polling, so false
    # if "succeeded" then we want to stop polling, so true
    # if "failed" then we want to stop polling, so true
    status = check_job_status(job_id)
    return status in ["succeeded", "failed"]


def is_success(job_id: str):
    # need to spit out the lowercase strings instead of real bools, allows the workflow to know if it succeeded or not
    if check_job_status(job_id) == "succeeded":
        return "true"
    else:
        raise ValueError("Job ran but did not succeed.")


def step_function(step: int) -> int:
    return random.randint(step, step + 10)


try:
    polling.poll(lambda: is_done(job_id), step=10, step_function=step_function, timeout=int(timeout))
    print(is_success(job_id))
except polling.TimeoutException as te:
    while not te.values.empty():
        # Print all of the values that did not meet the exception
        print(te.values.get(), file=sys.stderr)
