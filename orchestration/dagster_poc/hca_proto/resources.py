from dagster import resource, StringSource, Field
import time
import kubernetes
from uuid import uuid4
import subprocess
import sys

ONE_DAY_IN_SECONDS = 86400  # seconds
POLLING_INTERVAL = 5  # seconds


@resource({
    "project": Field(StringSource),
    "temp_location": Field(StringSource),
    "subnet_name": Field(StringSource),
    "service_account": Field(StringSource),
    "image_version": Field(StringSource),
    "namespace": Field(StringSource)
})
def dataflow_beam_runner(init_context):
    return DataflowBeamRunner(
        project=init_context.resource_config['project'],
        temp_location=init_context.resource_config['temp_location'],
        subnet_name=init_context.resource_config['subnet_name'],
        service_account=init_context.resource_config['service_account'],
        image_version=init_context.resource_config['image_version'],
        namespace=init_context.resource_config['namespace']
    )


@resource({
    "working_dir": Field(StringSource)
})
def local_beam_runner(init_context):
    return LocalBeamRunner(working_dir=init_context.resource_config["working_dir"])


class LocalBeamRunner:
    def __init__(self, working_dir):
        self.working_dir = working_dir

    def run(self, job_name, input_prefix, output_prefix, context):
        context.log.info("Local beam runner")
        subprocess.run(
            ["sbt", f'hca-transformation-pipeline/run --inputPrefix={input_prefix} --outputPrefix={output_prefix}'],
            check=True,
            cwd=f"{self.working_dir}"
        )


class DataflowBeamRunner:
    def __init__(self, project, temp_location, subnet_name, service_account, image_version, namespace):
        self.project = project
        self.temp_location = temp_location
        self.subnet_name = subnet_name
        self.service_account = service_account
        self.image_version = image_version
        self.namespace = namespace

    def run(self, job_name, input_prefix, output_prefix, context):
        args = [
            '--runner=dataflow',
            f"--inputPrefix={input_prefix}",
            f"--outputPrefix={output_prefix}",
            f"--project={self.project}",
            f"--region=us-central1",
            f"--tempLocation={self.temp_location}",
            f"--subnetwork=regions/us-central1/subnetworks/{self.subnet_name}",
            f"--serviceAccount={self.service_account}",
            f"--workerMachineType=n1-standard-4",
            f"--autoscalingAlgorithm=THROUGHPUT_BASED",
            f"--numWorkers=4",
            f"--maxNumWorkers=16",
            f"--experiments=shuffle_mode=service"
        ]

        image_name = f"us.gcr.io/broad-dsp-gcr-public/hca-transformation-pipeline:{self.image_version}"  # {context.solid_config['version']}"
        job = self.dispatch_k8s_job(self.namespace, image_name, job_name, args, context)
        context.log.info(f"job started: {job}")

        job_status = self.get_job_status(job.metadata.name, self.namespace)
        context.log.info(f"Job status = {job_status}")

        ## TODO: yuck
        timeout_seconds = ONE_DAY_IN_SECONDS
        start = time.time()
        while job_status.status.succeeded is None:
            context.log.info("Polling for success, none yet.")
            time.sleep(POLLING_INTERVAL)
            elapsed = time.time() - start

            job_status = self.get_job_status(job.metadata.name, self.namespace)
            if elapsed > timeout_seconds:
                # should check the job conditions payload instead
                context.log.error("Too much time elapsed, bailing out")
                raise Exception("too much time elapsed")

    @staticmethod
    def get_job_status(name, namespace):
        client = kubernetes.client.BatchV1Api()
        return client.read_namespaced_job_status(name, namespace)

    @staticmethod
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
