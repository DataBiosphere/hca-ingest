import subprocess
import uuid

from dagster import file_relative_path

from hca_orchestration.contrib.gcs import GsBucketWithPrefix


def submit_argo_job(dataset_id: str, dataset_name: str, gs_bucket_with_prefix: GsBucketWithPrefix) -> None:
    path = file_relative_path(__file__, '../../../workflows/dev/run-import-hca-total.yaml')

    # there is a --wait arg that theoretically blocks exit of the submit process until the workflow completes,
    # but it does not work in our testing (in my version at least). So, we fire-and-forget
    argo_params = [
        'argo',
        'submit',
        path,
        '-p', f'source-bucket-name={gs_bucket_with_prefix.bucket}',
        '-p', f'source-bucket-prefix=/{gs_bucket_with_prefix.prefix}',
        '-p', f'staging-bucket-prefix=dev_{uuid.uuid4().hex}',
        '-p', f'dataset-id={dataset_id}',
        '-p', f'data-repo-name=datarepo_{dataset_name}',
        '-p', 'data-repo-project=broad-jade-dev-data',
        '-p', 'data-repo-billing-profile-id=390e7a85-d47f-4531-b612-165fc977d3bd',
        '-p', 'data-repo-url=https://jade.datarepo-dev.broadinstitute.org/',
    ]
    subprocess.run(
        argo_params
    )
