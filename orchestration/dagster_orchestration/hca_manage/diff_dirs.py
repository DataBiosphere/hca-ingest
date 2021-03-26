"""
Compares the contents of two google storage paths via their md5
"""
import argparse
import logging

import google.auth
from google.cloud import storage

logging.basicConfig(level=logging.INFO)


def diff_dirs(project, source_bucket, source_prefix, target_bucket, target_prefix):
    creds, _ = google.auth.default()
    storage_client = storage.Client(project=project, credentials=creds)
    expected_blobs = {blob.name.replace(source_prefix, ''): blob.md5_hash
                      for blob in storage_client.list_blobs(source_bucket,
                                                            prefix=source_prefix)}

    logging.info(f"project = {project}, "
                 f"source_bucket = {source_bucket}, "
                 f"source_prefix = {source_prefix}, "
                 f"target_bucket = {target_bucket}, "
                 f"target_prefix = {target_prefix}")
    output_blobs = {blob.name.replace(target_prefix, ''): blob.md5_hash
                    for blob in storage_client.list_blobs(target_bucket,
                                                          prefix=target_prefix)}

    return expected_blobs, output_blobs


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--project")
    parser.add_argument("-sb", "--source_bucket")
    parser.add_argument("-sp", "--source_prefix")

    parser.add_argument("-tb", "--target_bucket")
    parser.add_argument("-tp", "--target_prefix")
    args = parser.parse_args()
    expected_blobs, output_blobs = diff_dirs(args.project, args.source_bucket,
                                             args.source_prefix, args.target_bucket, args.target_prefix)
    assert expected_blobs == output_blobs, "Output results differ from expected"