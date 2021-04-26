import unittest


class PreconfigurationSchemaTestCase(unittest.TestCase):
    def test_validated_config_raises_if_missing_keys(self):
        pass

    def test_validated_config_does_not_raise_if_missing_optional_keys(self):
        pass

    def test_validated_config_warns_and_cuts_if_extra_keys(self):
        pass

    def test_validated_config_returns_unaltered_if_all_keys_present(self):
        pass

    def test_load_files_raises_if_no_files_present(self):
        pass

    def test_load_files_returns_only_present_files(self):
        pass

    def test_load_for_mode_gives_precedent_to_mode_specific(self):
        pass

    def test_load_for_mode_works_with_global_only(self):
        pass

    def test_load_for_mode_returns_loaded_config(self):
        pass
