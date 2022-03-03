import logging
from typing import Any, Optional

from dagster_utils.contrib.google import parse_gs_path
from google.cloud.storage import Client
from hca.staging_area_validator import StagingAreaValidator

from hca_manage.common import DefaultHelpParser


class HcaValidator:
    def validate_staging_area(self, path: str, ignore_inputs: bool, client: Client) -> Any:
        """
        Run the validation pre-checks on the staging area
        :param path: Google Cloud Storage path for staging area
        """
        adapter = StagingAreaValidator(
            staging_area=path,
            ignore_dangling_inputs=ignore_inputs,
            validate_json=True
        )
        exit_code = adapter.main()
        if not exit_code:
            logging.info(f'Staging area {path} is valid')
        else:
            logging.error(f'Staging area {path} is invalid')

        return exit_code

    def validate_structure(self, path: str, gs_client: Client) -> int:
        well_known_dirs = {'/data', '/descriptors', '/links', '/metadata'}

        bucket_with_prefix = parse_gs_path(path)
        bucket = gs_client.bucket(bucket_with_prefix.bucket)

        exit_code = 0
        for well_known_dir in well_known_dirs:
            expected_blob = f"{bucket_with_prefix.prefix}{well_known_dir}"
            blobs = list(gs_client.list_blobs(bucket, prefix=expected_blob))
            if not blobs:
                logging.error(f"No {well_known_dir} dir found at path {path}")
                exit_code = 1

        return exit_code


# CLI
def run(arguments: Optional[list[str]] = None) -> None:
    parser = DefaultHelpParser(description="CLI to manage validate GS path and json files.")
    parser.add_argument("-p", "--path", help="GS path to validate", required=True)
    parser.add_argument("-i", "--ignore_inputs", help="Ignore input metadata files", default=False)
    args = parser.parse_args(arguments)

    client = Client()
    HcaValidator().validate_staging_area(args.path, args.ignore_inputs, client)


if __name__ == "__main__":
    run()
