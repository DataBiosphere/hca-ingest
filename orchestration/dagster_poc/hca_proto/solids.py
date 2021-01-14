from dagster import solid, Nothing, InputDefinition
import kubernetes
from uuid import uuid4
import time

ONE_DAY_IN_SECONDS = 86400

@solid(
    #config_schema={"gcs_prefix": str}
)
def clear_staging_dir(context) -> Nothing:
    # TODO this is POC placeholder, we need to actually clear the staging dir
    #context.log.info(f"--clear_staging_dir gcs_prefix={context.solid_config['gcs_prefix']}")
    pass


@solid(
    input_defs=[InputDefinition("start", Nothing)],
    # config_schema={
    #     "source_bucket_name": str,
    #     "source_bucket_prefix": str,
    #     "staging_bucket_prefix": str,
    #     #"args": list,
    #     "version": str,
    #     "timeout": int
    # }
)
def pre_process_metadata(context) -> Nothing:
    context.log.info(f"--pre_process_metadata")
    project = 'broad-dsp-monster-hca-dev'

    # TODO get this into config...maybe?
    args = [
        '--runner=dataflow',
        f"--inputPrefix=gs://fc-dcea072f-393f-4e20-8009-bf06ea594923/staging",
        f"--outputPrefix=gs://broad-dsp-monster-hca-dev-staging-storage/2020_01_14_arh_dagster_poc_test",
        f"--project=broad-dsp-monster-hca-dev",
        f"--region=us-central1",
        f"--tempLocation=gs://broad-dsp-monster-hca-dev-temp-storage/dataflow",
        f"--subnetwork=regions/us-central1/subnetworks/hca-network",
        f"--serviceAccount=hca-dataflow-runner@broad-dsp-monster-hca-dev.iam.gserviceaccount.com",
        f"--workerMachineType=n1-standard-4",
        f"--autoscalingAlgorithm=THROUGHPUT_BASED",
        f"--numWorkers=4",
        f"--maxNumWorkers=16",
        f"--experiments=shuffle_mode=service"
    ]

    image_name = f"us.gcr.io/broad-dsp-gcr-public/hca-transformation-pipeline:latest" #{context.solid_config['version']}"
    namespace = 'hca-mvp'
    job = dispatch_k8s_job(namespace, image_name, "pre-process-metadata", args, context)
    context.log.info(f"job started: {job}")

    job_status = get_job_status(job.metadata.name, namespace)
    context.log.info(f"Job status = {job_status}")

    ## TODO: yuck
    timeout_seconds = ONE_DAY_IN_SECONDS
    start = time.time()
    while job_status.status.succeeded is None:
        context.log.info("Polling for success, none yet.")
        context.log.info(f"Job status = {job_status}")
        time.sleep(1)
        elapsed = time.time() - start

        job_status = get_job_status(job.metadata.name, namespace)
        if elapsed > timeout_seconds:
            # should check the job conditions payload instead
            context.log.error("Too much time elapsed, bailing out")
            raise Exception("too much time elapsed")


def get_job_status(name, namespace):
    client = kubernetes.client.BatchV1Api()
    return client.read_namespaced_job_status(name, namespace)


def dispatch_k8s_job(namespace, image_name, job_name_prefix, args, context):
    # we will need to poll the pod/job status on creation
    kubernetes.config.load_kube_config()

    job_name = f"{job_name_prefix}-{uuid4()}"
    pod_name = f"{job_name}-pod"
    job_container = kubernetes.client.V1Container(
        name=job_name,
        image=image_name,
        args=args,
    )

    template = kubernetes.client.V1PodTemplateSpec(
        metadata=kubernetes.client.V1ObjectMeta(name=pod_name),
        spec=kubernetes.client.V1PodSpec(
            restart_policy="Never",
            containers=[job_container],
            service_account_name="argo-runner"
        ),
    )

    job = kubernetes.client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=kubernetes.client.V1ObjectMeta(
            name=job_name
        ),
        spec=kubernetes.client.V1JobSpec(
            template=template,
            backoff_limit=2,
            ttl_seconds_after_finished=86400
        ),
    )
    batch_v1 = kubernetes.client.BatchV1Api()
    api_response = batch_v1.create_namespaced_job(
        body=job,
        namespace=namespace)
    context.log.info(f"Job created. status='%s'" % str(api_response.status))

    return api_response
