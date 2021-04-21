import unittest

from dagster import configured
from hca_orchestration.resources import load_tag
from hca_orchestration.tests.support.resources import initialize_resource


class LoadTagTestCase(unittest.TestCase):

    def test_load_tag_with_suffix(self):
        configured_tag = configured(load_tag)({
            "load_tag_prefix": "fake_prefix",
            "append_timestamp": True
        })
        with initialize_resource(configured_tag) as tag:
            self.assertTrue(tag.startswith("fake_prefix"))

    def test_load_tag_no_suffix(self):
        configured_tag = configured(load_tag)({
            "load_tag_prefix": "fake_prefix",
            "append_timestamp": False
        })
        with initialize_resource(configured_tag) as tag:
            self.assertEqual(tag, "fake_prefix")
