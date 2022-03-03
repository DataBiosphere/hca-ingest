import pytest

from hca_orchestration.support.matchers import find_project_id_in_str


def test_find_project_id_in_str():
    test_str = "foo/b/ar/bbaz/4d6f6c96-2a83-43d8-8fe1-0f53bffd4674//bar/bar"
    project_id = find_project_id_in_str(test_str)

    assert project_id == '4d6f6c96-2a83-43d8-8fe1-0f53bffd4674'


def test_find_project_id_fails_multiples():
    test_str = "foo/b/ar/bbaz/4d6f6c96-2a83-43d8-8fe1-0f53bffd4674//bar/bar/88ec040b-8705-4f77-8f41-f81e57632f7d"

    with pytest.raises(Exception):
        find_project_id_in_str(test_str)


def test_find_project_id_fails_none():
    test_str = "example"

    with pytest.raises(Exception):
        find_project_id_in_str(test_str)
