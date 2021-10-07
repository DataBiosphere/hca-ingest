import argparse
import os
import sys

from dagster.utils import load_yaml_from_globs, file_relative_path
from dagster_graphql import DagsterGraphQLClient

SUPPORTED_CLI_ENVS = ["dev", "prod"]


def config_path(relative_path: str) -> str:
    path: str = file_relative_path(
        __file__, os.path.join("./config", relative_path)
    )
    return path


def run(input_prefix: str, dataset_id: str, env: str, dagster_host: str, dagster_port: int):
    config_dict = load_yaml_from_globs(
        config_path("load_hca_config.yaml")
    )
    config_dict['solids']['pre_process_metadata']['config']['input_prefix'] = input_prefix
    config_dict['resources']['target_hca_dataset']['config']['dataset_id'] = dataset_id

    client = DagsterGraphQLClient(dagster_host, port_number=dagster_port)
    run_id = client.submit_pipeline_execution(
        "load_hca",
        repository_location_name=f"{env}_repositories.py",
        repository_name="",
        mode=env,
        run_config=config_dict
    )
    print(f"Run submitted [id={run_id}]")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--env", required=True)
    parser.add_argument("-i", "--input_prefix", required=True)
    parser.add_argument("-d", "--dataset_id", required=True)
    parser.add_argument("-n", "--dagster_host", required=True)
    parser.add_argument("-p", "--dagster_port", required=True)

    args = parser.parse_args()

    if args.env not in SUPPORTED_CLI_ENVS:
        print(f"env must be in {SUPPORTED_CLI_ENVS}")
        sys.exit(1)

    run(args.input_prefix, args.dataset_id, args.env, args.dagster_host, int(args.dagster_port))
