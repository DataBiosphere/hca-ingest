from dagster import asset


@asset(
    group_name="hca",
    description="Metadata about the HCA project",
)
def hca_project_metadata():
    pass


@asset(group_name="hca", description="Data from the HCA staging bucket")
def hca_staging_data():
    pass
