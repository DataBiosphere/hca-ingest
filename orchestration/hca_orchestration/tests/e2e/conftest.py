# This serves to disable all of the e2e tests - see the README for more information.

import pytest

def pytest_collection_modifyitems(items):
    for item in items:
        item.add_marker(pytest.mark.skip(reason="Disabled all e2e tests - FE-203"))