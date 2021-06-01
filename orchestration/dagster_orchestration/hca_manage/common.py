import argparse
import csv
from dataclasses import dataclass
import functools
import sys
from typing import Any, Callable, NoReturn, TextIO, TypeVar, cast

from dagster import make_python_type_usable_as_dagster_type
from dagster.core.types.dagster_type import String as DagsterString

from dagster_utils.contrib.google import default_google_access_token
from dagster_utils.contrib.data_repo.typing import JobId
from data_repo_client import ApiClient, Configuration, RepositoryApi, ApiException

make_python_type_usable_as_dagster_type(JobId, DagsterString)


@dataclass
class ProblemCount:
    duplicates: int
    null_file_refs: int
    dangling_project_refs: int

    def has_problems(self) -> bool:
        return self.duplicates > 0 or self.null_file_refs > 0 or self.dangling_project_refs > 0


data_repo_host = {
    "dev": "https://jade.datarepo-dev.broadinstitute.org/",
    "prod": "https://jade-terra.datarepo-prod.broadinstitute.org/",
    "real_prod": "https://data.terra.bio/"
}

data_repo_profile_ids = {
    "dev": "390e7a85-d47f-4531-b612-165fc977d3bd",
    "prod": "db61c343-6dfe-4d14-84e9-60ddf97ea73f"
}


class DefaultHelpParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        """Print help message by default."""
        sys.stderr.write(f'error: {message}\n')
        self.print_help()
        sys.exit(2)


def get_api_client(host: str) -> RepositoryApi:
    # create API client
    config = Configuration(host=host)
    config.access_token = default_google_access_token()
    client = ApiClient(configuration=config)
    client.client_side_validation = False

    return RepositoryApi(api_client=client)


def get_dataset_id(dataset: str, data_repo_client: RepositoryApi) -> str:
    """
    Get the dataset ID of the provided dataset name.
    :return: The dataset id.
    """

    response = data_repo_client.enumerate_datasets(filter=dataset)
    return response.items[0].id  # type: ignore # data repo client has no type hints, since it's auto-generated


def populate_row_id_csv(row_ids: set[str], temp_file: TextIO) -> None:
    """
    Create a csv locally with one column filled with row ids to soft delete.
    :param row_ids: A set of row ids to soft delete.
    :param temp_file: a temporary file to pass in
    :return: The filename of the created csv.
    """
    sd_writer = csv.writer(temp_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)

    sd_writer.writerows([[rid] for rid in row_ids])


def query_yes_no(question: str, default: str = "no") -> bool:
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError(f"invalid default answer: '{default}'")

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


F = TypeVar("F", bound=Callable[..., Any])


def tdr_operation(func: F) -> F:
    """
    Wraps a TDR operation in exception handling for 401 errors
    :param op:
    :return:
    """

    @functools.wraps(func)
    def _tdr_wrapper(*args, **kwargs):  # type: ignore
        try:
            result = func(*args, **kwargs)
        except ApiException as e:
            if e.status == 401:
                sys.stderr.write(f"Permission denied, check your gcloud credentials\n")
            else:
                raise
        return result

    return cast(F, _tdr_wrapper)
