from datetime import datetime
import pytest

from hca_orchestration.support.dates import parse_version_to_datetime


def test_parse_version_to_datetime_valid_version():
    version = "2021-09-30T15:14:32.123456Z"
    parsed = parse_version_to_datetime(version)
    expected = datetime(2021, 9, 30, 15, 14, 32, 123456)
    assert parsed == expected, "Parsed entity version should match expected"


def test_parse_version_to_datetime_invalid_version():
    version = "2021-09-30T15:14:32.123"

    with pytest.raises(ValueError, match="does not match format"):
        parse_version_to_datetime(version)
