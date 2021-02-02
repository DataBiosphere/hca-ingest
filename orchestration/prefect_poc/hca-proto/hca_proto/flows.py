from prefect import Flow, Parameter
from tasks import clear_staging_area, run_dataflow_job, enumerate_jade_datasets, dummy_fan_out, dummy_transform, dummy_fan_in, dummy_load

with Flow("hca_proto") as flow:
    # parameters
    staging_bucket_name = Parameter("staging_bucket_name", required=True)
    staging_blob_name = Parameter("staging_blob_name", required=True)

    # imperative API defining step dependencies
    csa = clear_staging_area(staging_bucket_name, staging_blob_name)
    rdj = run_dataflow_job().set_dependencies(upstream_tasks=[csa])
    ejd = enumerate_jade_datasets().set_dependencies(upstream_tasks=[rdj])

    # mixing in functional API to show how it plays with the rest plus mapping/reducing
    raw = dummy_fan_out().set_dependencies(upstream_tasks=[ejd])
    transformed = dummy_transform.map(raw)
    reduced = dummy_fan_in(transformed)
    dummy_load(reduced)


# flow.register(project_name="testProj")
# flow.visualize()
flow.run(parameters={"staging_bucket_name": "broad-dsp-monster-hca-dev-staging-storage", "staging_blob_name": "nope"})
