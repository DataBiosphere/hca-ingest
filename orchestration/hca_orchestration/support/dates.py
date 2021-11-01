from datetime import datetime

HCA_VERSION_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
HCA_LEGACY_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
HCA_DATASET_DATETIME_FMT = "%Y%m%d"


def parse_version_to_datetime(version: str) -> datetime:
    try:
        return datetime.strptime(version, HCA_VERSION_FORMAT)
    except ValueError:
        return datetime.strptime(version, HCA_LEGACY_FORMAT)


def dataset_snapshot_formatted_date(dt: datetime) -> str:
    return dt.strftime(HCA_DATASET_DATETIME_FMT)
