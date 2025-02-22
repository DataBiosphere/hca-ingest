import time
from dagster import op

@op
def clear_staging_directory():
    time.sleep(4)
    pass

@op
def preprocess_metadata():
    time.sleep(4)
    pass

@op
def create_staging_dataset():
    time.sleep(4)
    pass

@op
def import_data_files():
    time.sleep(4)
    pass

@op
def fanout_file_metadata():
    time.sleep(4)
    pass

@op
def fanout_non_file_metadata():
    time.sleep(4)
    pass
