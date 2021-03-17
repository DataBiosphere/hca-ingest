from dataclasses import dataclass, field
import subprocess
from typing import List
from uuid import uuid4

from dagster import DagsterLogManager, Field, IntSource, resource, StringSource
from dagster.core.execution.context.init import InitResourceContext
from dagster_k8s.client import DagsterKubernetesClient

import kubernetes
from kubernetes.client.models.v1_job import V1Job


# separating out config for the cloud dataflow pipeline to make
# the giant mess of parameters here a little easier to parse
@dataclass
class DataflowCloudConfig:
    project: str
    service_account: str
    subnet_name: str
    region: str
    worker_machine_type: str
    starting_workers: int
    max_workers: int

    subnetwork: str = field(init=False)

    def __post_init__(self):
        self.subnetwork = '/'.join([
            'regions',
            self.cloud_config.region,
            'subnetworks',
            self.cloud_config.subnet_name,
        ])


@dataclass
class DataflowBeamRunner:
    cloud_config: DataflowCloudConfig
    temp_location: str
    image_name: str
    image_version: str
    namespace: str
    logger: DagsterLogManager

    def run(
        self,
        job_name: str,
        input_prefix: str,
        output_prefix: str
    ) -> None:
        args_dict = {
            'runner': 'dataflow',
            'inputPrefix': input_prefix,
            'outputPrefix': output_prefix,
            'project': self.cloud_config.project,
            'region': self.cloud_config.region,
            'tempLocation': self.temp_location,
            'subnetwork': self.cloud_config.subnetwork,
            'serviceAccount': self.cloud_config.service_account,
            'workerMachineType': self.cloud_config.worker_machine_type,
            'autoscalingAlgorithm': 'THROUGHPUT_BASED',
            'numWorkers': str(self.cloud_config.starting_workers),
            'maxNumWorkers': str(self.cloud_config.max_workers),
            'experiments': 'shuffle_mode=service',
        }

        args = [
            f'--{field_name}={value}'
            for field_name, value
            in args_dict.items()
        ]

        image_name = f"{self.image_name}:{self.image_version}"  # {context.solid_config['version']}"
        job = self.dispatch_k8s_job(image_name, job_name, args)
        self.logger.info("Dataflow job started")

        client = DagsterKubernetesClient.production_client()
        client.wait_for_job_success(job.metadata.name, self.namespace)

    def dispatch_k8s_job(self, image_name: str, job_name_prefix: str, args: List[str]) -> V1Job:
        # we will need to poll the pod/job status on creation
        kubernetes.config.load_kube_config()

        job_name = f"{job_name_prefix}-{uuid4()}"
        pod_name = f"{job_name}-pod"
        job_container = kubernetes.client.V1Container(
            name=job_name,
            image=image_name,
            args=args,
        )

        # TODO unhardcode the SA name below
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
            namespace=self.namespace)
        self.logger.info(f"Job created. status='{str(api_response.status)}'")

        return api_response


@resource({
    "project": Field(StringSource),
    "temp_location": Field(StringSource),
    "subnet_name": Field(StringSource),
    "region": Field(StringSource),
    "worker_machine_type": Field(StringSource),
    "starting_workers": Field(IntSource),
    "max_workers": Field(IntSource),
    "service_account": Field(StringSource),
    "image_name": Field(StringSource),
    "image_version": Field(StringSource),
    "namespace": Field(StringSource)
})
def dataflow_beam_runner(init_context: InitResourceContext):
    cloud_config = DataflowCloudConfig(
        project=init_context.resource_config['project'],
        service_account=init_context.resource_config['service_account'],
        subnet_name=init_context.resource_config['subnet_name'],
        region=init_context.resource_config['region'],
        worker_machine_type=init_context.resource_config['worker_machine_type'],
        starting_workers=init_context.resource_config['starting_workers'],
        max_workers=init_context.resource_config['max_workers'],
    )
    return DataflowBeamRunner(
        cloud_config=cloud_config,
        temp_location=init_context.resource_config['temp_location'],
        image_name=init_context.resource_config['image_name'],
        image_version=init_context.resource_config['image_version'],
        namespace=init_context.resource_config['namespace'],
        logger=init_context.log_manager,
    )


@dataclass
class LocalBeamRunner:
    working_dir: str
    logger: DagsterLogManager

    def run(
        self,
        job_name: str,
        input_prefix: str,
        output_prefix: str
    ) -> None:
        self.logger.info("Local beam runner")
        # TODO this is hardcoded to the HCA transformation pipeline for now
        subprocess.run(
            ["sbt", f'hca-transformation-pipeline/run --inputPrefix={input_prefix} --outputPrefix={output_prefix}'],
            check=True,
            cwd=self.working_dir
        )


@resource({
    "working_dir": Field(StringSource)
})
def local_beam_runner(init_context: InitResourceContext):
    return LocalBeamRunner(
        working_dir=init_context.resource_config["working_dir"],
        logger=init_context.log_manager,
    )


@resource
def test_beam_runner(init_context: InitResourceContext):
    class TestBeamRunner:
        def run(self, job_name: str, input_prefix: str, output_prefix: str):
            return None

    return TestBeamRunner()
