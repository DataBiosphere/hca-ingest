import os
from tempfile import TemporaryDirectory
import unittest
import yaml

from dagster import solid, Noneable, String

import hca_orchestration.config
from hca_orchestration.config import preconfigure_for_mode
from hca_orchestration.tests.support.filesystem import EphemeralNamedDirectory


class PreconfigureForModeTestCase(unittest.TestCase):
    def setUp(self):
        self.dummy_function = lambda _: 0
        self.dummy_function.__name__ = 'steve'
        self.config_package_dir = os.path.dirname(hca_orchestration.config.__file__)

    def test_treats_noneable_fields_as_optional(self):
        solid_def = solid(config_schema={
            'a': Noneable(String),
            'b': String,
        })(self.dummy_function)

        with TemporaryDirectory(dir=self.config_package_dir) as temp_dir:
            with open(os.path.join(temp_dir, 'mode.yaml'), 'w') as config_yaml_io:
                yaml.dump({'b': 'steve'}, config_yaml_io)

            preconfigured = preconfigure_for_mode(
                solid_def,
                'mode',
                subdirectory=os.path.basename(temp_dir))
            # assert that it accepts no additional config
            self.assertEqual(preconfigured.get_config_field().config_type.fields, {})

    def test_marks_non_noneable_fields_as_required(self):
        solid_def = solid(config_schema={
            'a': String,
            'b': String,
        })(self.dummy_function)

        with TemporaryDirectory(dir=self.config_package_dir) as temp_dir:
            with open(os.path.join(temp_dir, 'mode.yaml'), 'w') as config_yaml_io:
                yaml.dump({'b': 'steve'}, config_yaml_io)

            with self.assertRaises(ValueError):
                preconfigure_for_mode(
                    solid_def,
                    'mode',
                    subdirectory=os.path.basename(temp_dir))

    def test_defaults_to_using_resource_name_for_directory(self):
        solid_def = solid(config_schema={
            'a': String,
            'b': String,
        })(self.dummy_function)

        with EphemeralNamedDirectory('steve', self.config_package_dir) as temp_dir:
            with open(os.path.join(temp_dir, 'global.yaml'), 'w') as config_yaml_io:
                yaml.dump({'a': 'sneve', 'b': 'steve'}, config_yaml_io)

            preconfigured = preconfigure_for_mode(solid_def, 'mode')
            # assert that it accepts no additional config
            self.assertEqual(preconfigured.get_config_field().config_type.fields, {})

    def test_doesnt_require_fields_in_additional_schema(self):
        solid_def = solid(config_schema={
            'a': String,
            'b': String,
        })(self.dummy_function)

        with EphemeralNamedDirectory('steve', self.config_package_dir) as temp_dir:
            with open(os.path.join(temp_dir, 'global.yaml'), 'w') as config_yaml_io:
                yaml.dump({'a': 'sneve'}, config_yaml_io)

            preconfigured = preconfigure_for_mode(solid_def, 'mode', additional_schema={'b': String})
            # assert that it accepts no additional config
            fields = preconfigured.get_config_field().config_type.fields
            self.assertEqual(set(fields.keys()), {'b'})
