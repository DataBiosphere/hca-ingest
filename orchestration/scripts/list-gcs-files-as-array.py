from google.cloud import storage

import json
import os
import sys

bucket_name = os.environ["GCS_BUCKET"]
bucket_prefix = os.environ["GCS_PREFIX"]

# Connect to the bucket.
client = storage.Client()
bucket = client.get_bucket(bucket_name)

# List all blobs in the bucket, and add the 'gs://' prefix to their names.
blobs = bucket.list_blobs(prefix=bucket_prefix)
full_paths = [f'gs://{bucket_name}/{bucket_prefix}/{b.name}' for b in blobs]
indexed_paths = [{ 'id': i, 'path': p } for i, p in enumerate(full_paths)]

# Print the path-list as a JSON array so it an be slurped and scattered over by Argo.
print(json.dumps(indexed_paths))
