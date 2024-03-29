"""
TDR job inspection tool. Fetches the job status, and optionally the result
"""

import logging
import json

from hca_manage.common import DefaultHelpParser, get_api_client, data_repo_host, setup_cli_logging_format


def fetch_job_info() -> None:
    setup_cli_logging_format()
    parser = DefaultHelpParser(description="A simple CLI to inspect TDR jobs.")

    parser.add_argument("-e", "--env", help="The Jade environment to target",
                        choices=["dev", "prod", "real_prod"], required=True)
    parser.add_argument("-j", "--job_id", help="Jade job ID to get info for", required=True)
    parser.add_argument(
        "-r",
        "--fetch-result",
        help="Fetch job result if available",
        required=False,
        action="store_true")
    args = parser.parse_args()

    data_repo_client = get_api_client(data_repo_host[args.env])

    if args.fetch_result:
        result = data_repo_client.retrieve_job_result(args.job_id)
        logging.info(json.dumps(result, indent=4))
    else:
        logging.info(data_repo_client.retrieve_job(args.job_id))


if __name__ == '__main__':
    fetch_job_info()
