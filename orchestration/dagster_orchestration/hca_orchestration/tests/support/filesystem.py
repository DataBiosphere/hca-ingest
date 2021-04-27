from contextlib import contextmanager
import os
import shutil
from typing import Iterator


# works as a temporary directory, but with a fixed name
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
