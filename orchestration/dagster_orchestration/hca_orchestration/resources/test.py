from dagster import resource


@resource
def noop_data_repo_client(init_context):
    class NoopDataRepoClient():
        class NoopResult():
            def __init__(self, total):
                self.total = total

        def enumerate_datasets(self):
            return NoopDataRepoClient.NoopResult(5)

    return NoopDataRepoClient()


@resource
def local_storage_client(init_context):
    class MockBlob:
        def delete(self):
            pass

    class LocalStorageClient:
        def list_blobs(self, bucket_name: str, prefix: str):
            for _ in range(0, 10):
                yield MockBlob()

    return LocalStorageClient()


@resource
def test_beam_runner(init_context):
    class TestBeamRunner():
        def run(self, job_name, input_prefix, output_prefix, context):
            return None

    return TestBeamRunner()
