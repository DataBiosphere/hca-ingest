from datetime import datetime


HCA_VERSION_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def parse_version_to_datetime(version: str) -> datetime:
    return datetime.strptime(version, HCA_VERSION_FORMAT)


def release_date_format(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")
