from dagster import ModeDefinition, ResourceDefinition, SolidExecutionResult, execute_solid
from dagster_utils.contrib.data_repo.typing import JobId


from hca_orchestration.pipelines.load_hca import test_mode
from hca_orchestration.solids.load_hca.data_files.load_data_metadata_files import ingest_metadata_for_file_type, \
    ingest_metadata, file_metadata_fanout
from hca_orchestration.support.typing import HcaScratchDatasetName, MetadataType, MetadataTypeFanoutResult
from hca_orchestration.tests.support.gcs import FakeGCSClient, FakeGoogleBucket, HexBlobInfo

test_bucket = FakeGoogleBucket(
    {"gs://my-fake-bucket/fake-prefix": HexBlobInfo(hex_md5="b2d6ec45472467c836f253bd170182c7", content="test content")}
)

test_bucket_name = "my-fake-bucket"

load_metadata_test_mode: ModeDefinition = ModeDefinition(
    name="test",
    resource_defs={**test_mode.resource_defs}
)
load_metadata_test_mode.resource_defs["gcs"] = ResourceDefinition.hardcoded_resource(
    FakeGCSClient(
        buckets={test_bucket_name: test_bucket}
    )
)

run_config = {
    "resources": {
        "scratch_config": {
            "config": {
                "scratch_bucket_name": test_bucket_name,
                "scratch_prefix_name": "prefix_name",
                "scratch_bq_project": "bq_project",
                "scratch_dataset_prefix": "dataset_prefix",
                "scratch_table_expiration_ms": 86400000
            }
        },
        "target_hca_dataset": {
            "config": {
                "dataset_name": "dataset_name",
                "dataset_id": "dataset_id",
                "project_id": "project_id",
                "billing_profile_id": "billing_profile_id"
            }
        }
    }
}

metadata_fanout_result = MetadataTypeFanoutResult(
    scratch_dataset_name=HcaScratchDatasetName("dataset"),
    metadata_type=MetadataType("metadata"),
    path="path"
)


def test_ingest_metadata_for_file_type():
    result: SolidExecutionResult = execute_solid(
        ingest_metadata_for_file_type,
        mode_def=load_metadata_test_mode,
        input_values={
            "file_metadata_fanout_result": metadata_fanout_result
        },
        run_config=run_config
    )

    assert result.success


def test_ingest_metadata():
    result: SolidExecutionResult = execute_solid(
        ingest_metadata,
        mode_def=load_metadata_test_mode,
        input_values={
            "file_metadata_fanout_result": metadata_fanout_result
        },
        run_config=run_config
    )

    assert result.success


def test_file_metadata_fanout():
    result: SolidExecutionResult = execute_solid(
        file_metadata_fanout,
        mode_def=load_metadata_test_mode,
        input_values={
            "result": [JobId("abcdef")],
            "scratch_dataset_name": HcaScratchDatasetName("dataset")
        },
        run_config=run_config
    )

    assert result.success
