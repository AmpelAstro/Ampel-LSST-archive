import pytest


@pytest.fixture(scope="session")
def integration(pytestconfig):
    return pytestconfig.getoption("--integration")
