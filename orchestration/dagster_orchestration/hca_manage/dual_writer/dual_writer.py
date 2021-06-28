import argparse
from multiprocessing import Process

from hca_manage.dual_writer.argo_job import submit_argo_job
from hca_manage.dual_writer.dagster_job import submit_dagster_job
from hca_manage.dual_writer.db_diff import diff_dbs
from hca_orchestration.contrib.gcs import parse_gs_path


def _submit_jobs(args: argparse.Namespace) -> None:
    gs_bucket_with_path = parse_gs_path(args.input_prefix)
    argo_process = Process(target=submit_argo_job,
                           args=(args.argo_dataset_id, args.argo_dataset_name, gs_bucket_with_path))
    dagster_process = Process(target=submit_dagster_job,
                              args=(args.dagster_dataset_name, args.dagster_dataset_id, args.input_prefix))
    argo_process.start()
    dagster_process.start()
    dagster_process.join()


def run() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True, dest="cmd")

    run_jobs_parser = subparsers.add_parser("run_jobs")
    run_jobs_parser.add_argument("-di", "--dagster_dataset_id", required=True)
    run_jobs_parser.add_argument("-dn", "--dagster_dataset_name", required=True)
    run_jobs_parser.add_argument("-ai", "--argo_dataset_id", required=True)
    run_jobs_parser.add_argument("-an", "--argo_dataset_name", required=True)
    run_jobs_parser.add_argument("-ip", "--input_prefix", required=True)
    run_jobs_parser.set_defaults(func=_submit_jobs)

    db_diff_parser = subparsers.add_parser("diff_dbs")
    db_diff_parser.add_argument("-dn", "--dagster_dataset_name", required=True)
    db_diff_parser.add_argument("-an", "--argo_dataset_name", required=True)
    db_diff_parser.set_defaults(func=diff_dbs)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    run()
