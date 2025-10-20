import asyncio

from .models import SQLModel
from .server.db import get_engine
from .server.s3 import get_s3_bucket


async def _main() -> None:
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    bucket = await get_s3_bucket()
    if not await bucket.creation_date:
        await bucket.create()


def main() -> None:
    asyncio.run(_main())
