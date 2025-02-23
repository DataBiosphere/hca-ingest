from typing import ClassVar

from dagster import ConfigurableResource

from hca.contrib.validation_client import HcaValidator, LocalHcaValidator


class HcaValidatorResource(ConfigurableResource):
    validator: ClassVar[HcaValidator] = HcaValidator()


class LocalHcaValidatorResource(HcaValidatorResource):
    validator: ClassVar[LocalHcaValidator] = LocalHcaValidator()
