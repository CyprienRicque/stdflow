import stdflow as st


def test_exact_version():
    assert st.__version__ == "0.0.20"


def test_version():
    from stdflow import __version__

    assert isinstance(__version__, str), "Version must be a string"

    # Check if version follows semantic versioning format (major.minor.patch)
    major, minor, patch = map(int, __version__.split("."))
    assert major >= 0, "Major version must be non-negative integer"
    assert minor >= 0, "Minor version must be non-negative integer"
    assert patch >= 0, "Patch version must be non-negative integer"


if __name__ == "__main__":
    test_exact_version()
    test_version()
