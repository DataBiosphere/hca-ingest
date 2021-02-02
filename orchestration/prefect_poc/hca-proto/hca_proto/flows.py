from prefect import Flow, Parameter
from tasks import clear_staging_area, run_dataflow_job, enumerate_jade_datasets

with Flow("hca_proto") as flow:
    staging_bucket_name = Parameter("staging_bucket_name", required=True)
    staging_blob_name = Parameter("staging_blob_name", required=True)
    csa = clear_staging_area(staging_bucket_name, staging_blob_name)
    rdj = run_dataflow_job().set_dependencies(upstream_tasks=[csa])
    enumerate_jade_datasets().set_dependencies(upstream_tasks=[rdj])

# flow.register(project_name="testProj")
# flow.visualize()
flow.run(parameters={"staging_bucket_name": "broad-dsp-monster-hca-dev-staging-storage", "staging_blob_name": "nope"})
