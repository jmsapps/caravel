from importlib.metadata import version

import caravel


def test_package_exports_installed_version() -> None:
    assert caravel.__version__ == version("caravel")
