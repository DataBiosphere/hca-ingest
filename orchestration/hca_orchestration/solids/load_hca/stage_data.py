import uuid
from dagster import solid, InputDefinition, Nothing, String
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster_utils.resources.beam.beam_runner import BeamRunner
from google.cloud.bigquery import Dataset
from google.cloud.storage.client import Client

from hca_orchestration.support.typing import HcaScratchDatasetName
from hca_orchestration.models.hca_dataset import TdrDataset


@solid(
    required_resource_keys={"gcs", "scratch_config"},
    input_defs=[InputDefinition("ignore", Nothing)]
)
def clear_scratch_dir(context: AbstractComputeExecutionContext) -> int:
    """
    Given a staging bucket + prefix, deletes all blobs present at that path
    :return: Number of deletions
    """
    scratch_bucket_name = context.resources.scratch_config.scratch_bucket_name
    scratch_prefix_name = context.resources.scratch_config.scratch_prefix_name

    context.log.info(f"Clearing scratch dir at {scratch_prefix_name}")
    deletions_count = clear_dir(scratch_bucket_name, scratch_prefix_name, context.resources.gcs)
    context.log.info(f"Deleted {deletions_count} blobs under {scratch_prefix_name}")
    return deletions_count


def clear_dir(bucket: str, prefix: str, gcs: Client) -> int:
    blobs = gcs.list_blobs(bucket, prefix=f"{prefix}/")
    deletions_count = 0
    for blob in blobs:
        blob.delete()
        deletions_count += 1
    return deletions_count


@solid(
    required_resource_keys={"beam_runner", "scratch_config"},
    config_schema={
        "input_prefix": String,
    },
    input_defs=[InputDefinition("start", Nothing)],
)
def pre_process_metadata(context: AbstractComputeExecutionContext) -> Nothing:
    """
    Runs the Beam hca transformation pipeline flow over the given input prefix
    """
    context.log.info("--pre_process_metadata")

    # not strictly required, but makes the ensuing lines a lot shorter
    bucket_name = context.resources.scratch_config.scratch_bucket_name
    prefix_name = f"{context.resources.scratch_config.scratch_prefix_name}"

    beam_runner: BeamRunner = context.resources.beam_runner
    args_dict = {
        "inputPrefix": context.solid_config["input_prefix"],
        "outputPrefix": f'gs://{bucket_name}/{prefix_name}',
    }
    run_id = context.run_id
    if not run_id:
        run_id = uuid.uuid4().hex
    tag = f"{run_id[0:8]}"

    beam_runner.run(
        run_arg_dict=args_dict,
        job_name=f"hca-{tag}",
        target_class="org.broadinstitute.monster.hca.HcaPipeline",
        scala_project="hca-transformation-pipeline",
    )


@solid(
    required_resource_keys={"bigquery_client", "load_tag", "scratch_config", "target_hca_dataset"},
    input_defs=[InputDefinition("start", Nothing)],
)
def create_scratch_dataset(context: AbstractComputeExecutionContext) -> HcaScratchDatasetName:
    """
    Creates a staging dataset that will house records for update/insertion into the
    final TDR dataset
    :return: Name of the staging dataset
    """
    scratch_bq_project = context.resources.scratch_config.scratch_bq_project
    scratch_dataset_prefix = context.resources.scratch_config.scratch_dataset_prefix
    load_tag = context.resources.load_tag
    target_hca_dataset: TdrDataset = context.resources.target_hca_dataset

    dataset_name = f"{scratch_bq_project}.{scratch_dataset_prefix}_{load_tag}"

    dataset = Dataset(dataset_name)

    # co-locate the staging dataset in the same BQ location as the target TDR dataset
    # so we can perform joins
    dataset.location = target_hca_dataset.bq_location
    dataset.default_table_expiration_ms = context.resources.scratch_config.scratch_table_expiration_ms

    bq_client = context.resources.bigquery_client
    bq_client.create_dataset(dataset)

    context.log.info(f"Created scratch dataset {dataset_name}")

    return HcaScratchDatasetName(dataset_name)
