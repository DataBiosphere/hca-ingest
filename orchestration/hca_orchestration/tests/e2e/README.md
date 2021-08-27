## End-to-end Pipeline Tests

The tests in this directory are intended to be a full end-to-end test of the HCA pipeline. 
They run against a real dataset, using a local beam runner and staged fixture data in
GCS. They are excluded from the default unit test suite given how long they take to run,
but are run as part of CI (and are completely runnable locally).

### To run locally:

`pytest -s -v test_load_hca.py`