from data_repo_client import ApiClient, Configuration, RepositoryApi, EnumerateDatasetModel
import google.auth
from google.auth.transport.requests import Request
from google.cloud import storage
import prefect
from prefect import task


@task
def clear_staging_area(bucket_name, blob_name):
    credentials, project = google.auth.default()

    storage_client = storage.Client(project=project, credentials=credentials)

    blobs = storage_client.list_blobs(bucket_name, prefix=f"{blob_name}/")
    count = 0
    for count, blob in enumerate(blobs):
        blob.delete()
    logger = prefect.context.get("logger")
    logger.debug(f"--clear_staging_dir found {count} blobs to delete under {blob_name}")


# show ability to run container in k8s
@task
def run_dataflow_job():
    return 1


@task
def enumerate_jade_datasets():
    # get token for jade, assumes application default credentials work for specified environment
    credentials, _ = google.auth.default()
    auth_req = Request()
    credentials.refresh(auth_req)

    # create API client
    config = Configuration(host="https://jade.datarepo-dev.broadinstitute.org/")
    config.access_token = credentials.token
    client = ApiClient(configuration=config)
    client.client_side_validation = False

    # submit file ingest (for now just enumerate datasets or something to prove interaction works)
    repoApi = RepositoryApi(api_client=client)

    datasets = repoApi.enumerate_datasets()

    logger = prefect.context.get("logger")
    logger.debug(f"Enumerate found {datasets.total} datasets in the repo")


# show ability to fan out
def dummy_fan_out():
    pass


# show ability to fan back in
def dummy_fan_in():
    pass
