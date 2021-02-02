from prefect import Client


def create_project():
    client = Client()
    client.create_project(project_name="testProj")
