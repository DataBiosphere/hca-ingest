from datetime import datetime


HCA_VERSION_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def parse_version_to_datetime(version: str) -> datetime:
    return datetime.strptime(version, HCA_VERSION_FORMAT)
