from .models import SQLModel
from .server.db import get_engine
from .server.s3 import get_s3_bucket


def main() -> None:
    engine = get_engine()
    SQLModel.metadata.create_all(engine)
    bucket = get_s3_bucket()
    if not bucket.creation_date:
        bucket.create()
