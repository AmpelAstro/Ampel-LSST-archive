"""earthdistance

Revision ID: 2157b31be5c5
Revises:
Create Date: 2025-10-22 21:18:21.572257

"""

from collections.abc import Sequence

from alembic_utils.pg_extension import PGExtension
from alembic_utils.pg_function import PGFunction

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2157b31be5c5"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

cube_extension = PGExtension(schema="public", signature="cube")
earthdistance_extension = PGExtension(schema="public", signature="earthdistance")


def upgrade() -> None:
    """Upgrade schema."""
    op.create_entity(cube_extension)
    op.create_entity(earthdistance_extension)
    # redefine earth radius such that great circle distances are in degrees
    op.replace_entity(
        PGFunction(
            schema="public",
            signature="earth() RETURNS float8",
            definition="""
                LANGUAGE SQL IMMUTABLE PARALLEL SAFE
                AS 'SELECT 180/pi()'
            """,
        )
    )
    # schema-qualify definition of ll_to_earth() so that it can used for autoanalyze
    op.replace_entity(
        PGFunction(
            schema="public",
            signature="ll_to_earth(float8, float8) RETURNS earth",
            definition="""
                LANGUAGE SQL
                IMMUTABLE STRICT
                PARALLEL SAFE
                AS
                'SELECT public.cube(public.cube(public.cube(public.earth()*cos(radians($1))*cos(radians($2))),public.earth()*cos(radians($1))*sin(radians($2))),public.earth()*sin(radians($1)))::public.earth'
            """,
        )
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_entity(earthdistance_extension)
    op.drop_entity(cube_extension)
