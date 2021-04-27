from contextlib import contextmanager
from dataclasses import dataclass
from importlib.util import find_spec
import os
from pathlib import Path
import random
import shutil
from typing import ContextManager, Iterator, Optional


def random_valid_package_name(count):
    chars = 'abcdefghijklmnopqrstuvwxyz_'
    return ''.join(random.choices(chars, k=count))


@dataclass
class TempPackage:
    directory: str
    package: str
    subpackage: str


# works as a temporary directory, but with a fixed name. we use this because the name generation
# for TemporaryDirectory can produce invalid package names
@contextmanager
def EphemeralNamedDirectory(dirname: str, parent_directory: str) -> Iterator[str]:
    if dirname == '' or parent_directory == '':
        raise ValueError('Must provide a directory name to create!')

    target_path = os.path.join(parent_directory, dirname)
    if os.path.exists(target_path):
        raise FileExistsError(f"Tried to create an ephemeral directory at existing path {target_path}")

    try:
        os.mkdir(target_path)
        yield target_path
    finally:
        shutil.rmtree(target_path)


# generate a temporary directory that is a valid python package (i.e. contains an __init__.py file)
# beneath the specified parent package
@contextmanager
def TemporaryPackage(parent_package: str, exact_name: Optional[str] = None) -> Iterator[TempPackage]:
    # get the absolute path of the specified package
    if (package_spec := find_spec(parent_package)) and package_spec.submodule_search_locations:
        package_path = package_spec.submodule_search_locations[0]
    else:
        raise ModuleNotFoundError(
            f"Failed to locate parent package {parent_package} when generating temporary package."
        )

    temp_dir: ContextManager[str]

    temp_dir = EphemeralNamedDirectory(exact_name or random_valid_package_name(8), package_path)

    # make a new temporary subdirectory beneath it
    with temp_dir as temp_package_dir:
        # put an __init__.py file in the subdir so it's treated as a package
        Path(os.path.join(temp_package_dir, '__init__.py')).touch()
        subpackage = os.path.basename(temp_package_dir)
        yield TempPackage(
            directory=temp_package_dir,
            package='.'.join([parent_package, subpackage]),
            subpackage=subpackage,
        )
