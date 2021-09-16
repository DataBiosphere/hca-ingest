import argparse
import logging
from typing import Optional
import uuid

from hca_manage.common import setup_cli_logging_format, DefaultHelpParser, get_resources_api_client, data_repo_host


def run(arguments: Optional[list[str]] = None) -> None:
    setup_cli_logging_format()
    parser = DefaultHelpParser(description="A simple CLI to manage TDR billing profiles.")
    parser.add_argument("-e", "--env", help="The Jade environment to target", choices=data_repo_host.keys(),
                        required=True)
    subparsers = parser.add_subparsers()

    # create
    billing_profile_create = subparsers.add_parser("create")
    billing_profile_create.add_argument("-a", "--billing_account_id", required=True)
    billing_profile_create.add_argument("-d", "--description", required=True)
    billing_profile_create.add_argument("-n", "--profile_name", required=True)
    billing_profile_create.set_defaults(func=_create_billing_profile)

    billing_profile_query = subparsers.add_parser("enumerate")
    billing_profile_query.set_defaults(func=_enumerate_billing_profiles)

    billing_profile_retrieve = subparsers.add_parser("retrieve")
    billing_profile_retrieve.add_argument("-i", "--billing_profile_id", required=True)
    billing_profile_retrieve.set_defaults(func=_retrieve_billing_profile)

    args = parser.parse_args(arguments)
    args.func(args)


def _create_billing_profile(args: argparse.Namespace) -> None:
    host = data_repo_host[args.env]
    resources_api_client = get_resources_api_client(host)

    profile_id = str(uuid.uuid4())
    logging.info(f"Creating billing profile with id = {profile_id}")
    billing_profile_request = {
        "biller": "direct",
        "billingAccountId": args.billing_account_id,
        "description": args.description,
        "id": profile_id,
        "profileName": args.profile_name
    }
    response = resources_api_client.create_profile(
        billing_profile_request=billing_profile_request
    )

    logging.info(f"Billing profile creation response = {response}")


def _enumerate_billing_profiles(args: argparse.Namespace) -> None:
    host = data_repo_host[args.env]
    resources_api_client = get_resources_api_client(host)

    response = resources_api_client.enumerate_profiles()

    logging.info(response)


def _retrieve_billing_profile(args: argparse.Namespace) -> None:
    host = data_repo_host[args.env]
    resources_api_client = get_resources_api_client(host)

    response = resources_api_client.retrieve_profile(id=args.billing_profile_id)

    logging.info(response)


if __name__ == '__main__':
    run()
