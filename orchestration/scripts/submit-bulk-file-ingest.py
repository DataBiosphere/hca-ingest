import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import os

credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])

base_url = os.environ["API_URL"]
dataset_id = os.environ["DATASET_ID"]
profile_id = os.environ["PROFILE_ID"]
control_file = os.environ["INPUT_PATH"]
load_tag = os.environ["LOAD_TAG"]

authed_session = AuthorizedSession(credentials)


def submit_job(dataset_id: str, **kwargs):
  response = authed_session.post(f'{base_url}/api/repository/v1/datasets/{dataset_id}/files/bulk', json=kwargs)
  if response.ok:
    return response.json()['id']
  else:
    raise HTTPError(f'Bad response, got code of: {response.status_code}')

print(submit_job(dataset_id, profileId=profile_id, loadControlFile=control_file, loadTag=load_tag, maxFailedFileLoads=0))
