from unittest.mock import Mock

from dagster import ModeDefinition, ResourceDefinition, SolidExecutionResult, execute_solid, resource

from hca_orchestration.contrib.bigquery import BigQueryService
from hca_orchestration.pipelines.load_hca import test_mode
from hca_orchestration.solids.load_hca.load_table import check_has_data, clear_outdated, \
    load_table, start_load
from hca_orchestration.support.typing import HcaScratchDatasetName, MetadataType, MetadataTypeFanoutResult
from hca_orchestration.tests.support.gcs import FakeGCSClient, FakeGoogleBucket, HexBlobInfo

test_bucket = FakeGoogleBucket(
    {"gs://my-fake-bucket/fake-prefix": HexBlobInfo(hex_md5="b2d6ec45472467c836f253bd170182c7", content="test content")}
)

test_bucket_name = "my-fake-bucket"

load_table_test_mode = ModeDefinition(
    "test_load_table",
    resource_defs={**test_mode.resource_defs}
)
load_table_test_mode.resource_defs["gcs"] = ResourceDefinition.hardcoded_resource(
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


def test_load_table():
    result: SolidExecutionResult = execute_solid(
        load_table,
        mode_def=load_table_test_mode,
        input_values={
            "metadata_fanout_result": metadata_fanout_result
        },
        run_config=run_config
    )

    assert result.success

#
# def test_clear_outdated():
#     result: SolidExecutionResult = execute_solid(
#         clear_outdated,
#         mode_def=load_table_test_mode,
#         input_values={
#             "has_outdated": True,
#             "metadata_fanout_result": metadata_fanout_result
#         },
#         run_config=run_config
#     )
#
#     assert result.success
#     assert result.output_value() == "abcdef"
#
#
# def test_check_has_data_false():
#     # need a bucket with a blob that has size 0, aka no content/empty content
#     this_test_bucket = FakeGoogleBucket(
#         {"gs://my-fake-bucket/fake-prefix": HexBlobInfo(hex_md5="b2d6ec45472467c836f253bd170182c7",
#                                                         content="")}
#     )
#
#     this_test_mode = ModeDefinition(
#         "test_check_has_data_mode",
#         resource_defs={**load_table_test_mode.resource_defs}
#     )
#     this_test_mode.resource_defs["gcs"] = ResourceDefinition.hardcoded_resource(
#         FakeGCSClient(
#             buckets={test_bucket_name: this_test_bucket}
#         )
#     )
#
#     result: SolidExecutionResult = execute_solid(
#         check_has_data,
#         mode_def=this_test_mode,
#         input_values={
#             "metadata_fanout_result": metadata_fanout_result
#         },
#         run_config=run_config
#     )
#
#     assert result.success
#     assert not result.output_value("no_data")
#
#
# def test_check_has_data_true():
#     result: SolidExecutionResult = execute_solid(
#         check_has_data,
#         mode_def=load_table_test_mode,
#         input_values={
#             "metadata_fanout_result": metadata_fanout_result
#         },
#         run_config=run_config
#     )
#
#     assert result.success
#     assert result.output_value("has_data")

#
# def test_check_has_outdated_false():
#     # need a bucket with a blob that has size 0, aka no content/empty content
#     this_test_bucket = FakeGoogleBucket(
#         {"gs://my-fake-bucket/fake-prefix": HexBlobInfo(hex_md5="b2d6ec45472467c836f253bd170182c7",
#                                                         content="")}
#     )
#
#     this_test_mode = ModeDefinition(
#         "test_check_has_outdated_mode",
#         resource_defs={**load_table_test_mode.resource_defs}
#     )
#     this_test_mode.resource_defs["gcs"] = ResourceDefinition.hardcoded_resource(
#         FakeGCSClient(
#             buckets={test_bucket_name: this_test_bucket}
#         )
#     )
#
#     result: SolidExecutionResult = execute_solid(
#         check_has_outdated,
#         mode_def=this_test_mode,
#         input_values={
#             "parent_load_job_id": JobId("abcdef"),
#             "metadata_fanout_result": metadata_fanout_result
#         },
#         run_config=run_config
#     )
#
#     assert result.success
#     assert not result.output_value("no_outdated")
#
#
# def test_check_has_outdated_true():
#     result: SolidExecutionResult = execute_solid(
#         check_has_outdated,
#         mode_def=load_table_test_mode,
#         input_values={
#             "parent_load_job_id": JobId("abcdef"),
#             "metadata_fanout_result": metadata_fanout_result
#         },
#         run_config=run_config
#     )
#
#     assert result.success
#     assert result.output_value("has_outdated")

#
# def test_start_load_yes_new_rows():
#     @resource
#     def _mock_bq_service(_init_context) -> BigQueryService:
#         svc = Mock(spec=BigQueryService)
#         svc.get_num_rows_in_table = Mock(return_value=1)
#         return svc
#
#     this_test_mode = ModeDefinition(
#         "test_start_load",
#         resource_defs={**load_table_test_mode.resource_defs}
#     )
#     this_test_mode.resource_defs["bigquery_service"] = _mock_bq_service
#
#     result: SolidExecutionResult = execute_solid(
#         start_load,
#         mode_def=this_test_mode,
#         input_values={
#             "has_data": True,
#             "metadata_fanout_result": metadata_fanout_result
#         },
#         run_config=run_config
#     )
#
#     assert result.success
#     assert result.output_value("job_id") == "abcdef"
#
#
# def test_start_load_no_new_rows():
#     result: SolidExecutionResult = execute_solid(
#         start_load,
#         mode_def=load_table_test_mode,
#         input_values={
#             "has_data": True,
#             "metadata_fanout_result": metadata_fanout_result
#         },
#         run_config=run_config
#     )
#
#     assert result.success
#     assert result.output_value("no_job") == "no_job"
