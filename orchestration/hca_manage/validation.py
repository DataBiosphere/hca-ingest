import logging
from typing import Optional

from hca_manage.common import DefaultHelpParser
from hca.staging_area_validator import StagingAreaValidator


class HcaValidator:
    def validate_staging_area(self, path: str, ignore_inputs: bool) -> int:
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


## CLI
def run(arguments: Optional[list[str]] = None) -> None:
    parser = DefaultHelpParser(description="CLI to manage validate GS path and json files.")
    parser.add_argument("-p", "--path", help="GS path to validate", required=True)
    parser.add_argument("-i", "--ignore_inputs", help="Ignore input metadata files", default=False)
    args = parser.parse_args(arguments)

    HcaValidator().validate_staging_area(args.path, args.ignore_inputs)


if __name__ == "__main__":
    run()
