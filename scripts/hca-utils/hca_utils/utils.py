class HcaUtils:
    def __init__(self, environment: str, project: str, dataset: str):
        self.environment = environment

        if environment == "dev":
            self.project = "broad-jade-dev-data"
        else:
            self.project = project

        self.dataset = dataset
