import asyncio

from sqlalchemy import DDL

from .models import SQLModel
from .server.db import get_engine
from .server.s3 import get_s3_bucket

extensions = """
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;

/* redefine earth radius such that great circle distances are in degrees */
CREATE OR REPLACE FUNCTION earth() RETURNS float8
LANGUAGE SQL IMMUTABLE PARALLEL SAFE
AS 'SELECT 180/pi()';

/* schema-qualify definition of ll_to_earth() so that it can used for autoanalyze */
CREATE OR REPLACE FUNCTION ll_to_earth(float8, float8)
RETURNS earth
LANGUAGE SQL
IMMUTABLE STRICT
PARALLEL SAFE
AS 'SELECT public.cube(public.cube(public.cube(public.earth()*cos(radians($1))*cos(radians($2))),public.earth()*cos(radians($1))*sin(radians($2))),public.earth()*sin(radians($1)))::public.earth';
"""


async def _main() -> None:
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
        for line in extensions.strip().split(";"):
            if line.strip():
                await conn.execute(DDL(line))
    bucket = await get_s3_bucket()
    if not await bucket.creation_date:
        await bucket.create()


def main() -> None:
    asyncio.run(_main())
