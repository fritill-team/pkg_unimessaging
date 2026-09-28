from importlib.metadata import version

import unimessaging


def test_version_matches_installed_distribution():
    assert unimessaging.__version__ == version("unimessaging")
