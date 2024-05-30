## End-to-end Pipeline Tests

The tests in this directory are intended to be a full end-to-end test of the HCA pipeline. 
They run against a real dataset, using a local beam runner and staged fixture data in
GCS. They are excluded from the default unit test suite given how long they take to run,
but are run as part of CI (and are completely runnable locally).

Note that these currently being skipped and will be re-enabled when either there is\
time to refactor to use TDR prod or we've migrated (and will use TDR prod) - see FE-203 & FE-204 for more details.

(rename conftest_orig.py to conftest.py to run these tests)

### To run locally:

`pytest -s -v test_load_hca.py`