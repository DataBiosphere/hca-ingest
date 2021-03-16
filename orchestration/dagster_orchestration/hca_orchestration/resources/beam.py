from dataclasses import dataclass
import subprocess
from typing import Any, List
from uuid import uuid4

from dagster import resource, StringSource, Field
from dagster_k8s.client import DagsterKubernetesClient

import kubernetes


@dataclass
class DataflowBeamRunner:
    project: str
    temp_location: str
    subnet_name: str
    service_account: str
    image_name: str
    image_version: str
    namespace: str

    def run(
        self,
        job_name: str,
        input_prefix: str,
        output_prefix: str,
        context: Any
    ) -> None:
        args = [
            '--runner=dataflow',
            f"--inputPrefix={input_prefix}",
            f"--outputPrefix={output_prefix}",
            f"--project={self.project}",
            "--region=us-central1",
            f"--tempLocation={self.temp_location}",
            f"--subnetwork=regions/us-central1/subnetworks/{self.subnet_name}",
            f"--serviceAccount={self.service_account}",
            "--workerMachineType=n1-standard-4",
            "--autoscalingAlgorithm=THROUGHPUT_BASED",
            "--numWorkers=4",
            "--maxNumWorkers=16",
            "--experiments=shuffle_mode=service"
        ]

        image_name = f"{self.image_name}:{self.image_version}"  # {context.solid_config['version']}"
        job = self.dispatch_k8s_job(self.namespace, image_name, job_name, args, context)
        context.log.info("Dataflow job started")

        DataflowBeamRunner.get_job_status(job.metadata.name, self.namespace)
        client = DagsterKubernetesClient.production_client()
        client.wait_for_job_success(job.metadata.name, self.namespace)

    @staticmethod
    def get_job_status(name: str, namespace: str) -> str:
        client = kubernetes.client.BatchV1Api()
        return client.read_namespaced_job_status(name, namespace)  # type: ignore # (un-annotated library)

    @staticmethod
    def dispatch_k8s_job(namespace: str, image_name: str, job_name_prefix: str, args: List[str], context):
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
            namespace=namespace)
        context.log.info(f"Job created. status='{str(api_response.status)}'")

        return api_response


@resource({
    "project": Field(StringSource),
    "temp_location": Field(StringSource),
    "subnet_name": Field(StringSource),
    "service_account": Field(StringSource),
    "image_name": Field(StringSource),
    "image_version": Field(StringSource),
    "namespace": Field(StringSource)
})
def dataflow_beam_runner(init_context):
    return DataflowBeamRunner(
        project=init_context.resource_config['project'],
        temp_location=init_context.resource_config['temp_location'],
        subnet_name=init_context.resource_config['subnet_name'],
        service_account=init_context.resource_config['service_account'],
        image_name=init_context.resource_config['image_name'],
        image_version=init_context.resource_config['image_version'],
        namespace=init_context.resource_config['namespace']
    )


@dataclass
class LocalBeamRunner:
    working_dir: str

    def run(self, job_name: str, input_prefix: str, output_prefix: str, context) -> None:
        context.log.info("Local beam runner")
        # TODO this is hardcoded to the HCA transformation pipeline for now
        subprocess.run(
            ["sbt", f'hca-transformation-pipeline/run --inputPrefix={input_prefix} --outputPrefix={output_prefix}'],
            check=True,
            cwd=f"{self.working_dir}"
        )


@resource({
    "working_dir": Field(StringSource)
})
def local_beam_runner(init_context):
    return LocalBeamRunner(working_dir=init_context.resource_config["working_dir"])


@resource
def test_beam_runner(init_context):
    class TestBeamRunner:
        def run(self, job_name: str, input_prefix: str, output_prefix: str, context):
            return None

    return TestBeamRunner()
