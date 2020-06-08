from collections import namedtuple
import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import os

Counts = namedtuple('Counts', ['succeeded', 'failed', 'not_tried'])
credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])

base_url = os.environ["API_URL"]
job_id = os.environ["JOB_ID"]

authed_session = AuthorizedSession(credentials)


def get_counts(job_id: str) -> Counts:
  response = authed_session.get(f'{base_url}/api/repository/v1/jobs/{job_id}/result')
  if response.ok:
    json = response.json()
    return Counts(
      succeeded=json['succeededFiles'],
      failed=json['failedFiles'],
      not_tried=json['notTriedFiles'])
  else:
    raise HTTPError(f'Bad response, got code of: {response.status_code}')


counts = get_counts(job_id)
if counts.failed != 0:
  raise Exception(f'Bulk load failed on {counts.failed} files ({counts.not_tried} not tried, {counts.succeeded} successful)')
