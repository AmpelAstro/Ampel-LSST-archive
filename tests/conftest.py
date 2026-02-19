import os

pytest_plugins = ["tests.fixtures"]

os.environ["CATALOG_ENDPOINT_URL"] = "http://FIXME-CATALOG_ENDPOINT_URL-MUST-BE-SET"
os.environ["S3_ENDPOINT"] = "http://FIXME-S3_ENDPOINT-MUST-BE-SET"


def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="run docker-based integration tests",
    )
    parser.addoption(
        "--no-integration",
        dest="integration",
        action="store_false",
        help="disable docker-based integration tests",
    )
