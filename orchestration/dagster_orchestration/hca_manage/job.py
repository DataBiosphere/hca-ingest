import logging
import json

from hca_manage.common import DefaultHelpParser, get_api_client, data_repo_host

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_job_info():
    parser = DefaultHelpParser(description="A simple CLI to manage TDR datasets.")

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
    else:
        result = data_repo_client.retrieve_job(args.job_id)
    logging.info(json.dumps(result, indent=4))


if __name__ == '__main__':
    fetch_job_info()
