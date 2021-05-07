import unittest
from unittest.mock import Mock

from hca_orchestration.contrib.retry import retry, is_truthy, RetryException


class RetryTestCase(unittest.TestCase):
    def test_retry_no_result(self):
        test_fn = Mock()

        retry(test_fn, 1, 1, lambda _: True)
        test_fn.assert_called_once()

    def test_retry_with_result(self):
        test_fn = Mock()
        test_fn.return_value = "foo"

        result = retry(test_fn, 1, 1, is_truthy, "bar", "baz")
        test_fn.assert_called_once_with("bar", "baz")
        self.assertEqual(result, "foo")

    def test_retry_timeout(self):
        test_fn = Mock()
        test_fn.return_value = False

        with self.assertRaises(RetryException):
            retry(test_fn, 1, 1, is_truthy, "bar")

        test_fn.assert_called_with("bar")
