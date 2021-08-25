import unittest

from dagster import configured
from dagster_utils.testing.resources import initialize_resource

from hca_orchestration.resources import load_tag


class LoadTagTestCase(unittest.TestCase):

    def test_load_tag_with_suffix(self):
        configured_tag = configured(load_tag)({
            "load_tag_prefix": "fake",
            "append_timestamp": True
        })
        with initialize_resource(configured_tag) as tag:
            self.assertTrue(tag.startswith("fake"))

    def test_load_tag_no_suffix(self):
        configured_tag = configured(load_tag)({
            "load_tag_prefix": "fake_prefix",
            "append_timestamp": False
        })
        with initialize_resource(configured_tag) as tag:
            self.assertEqual(tag, "fake_prefix")
