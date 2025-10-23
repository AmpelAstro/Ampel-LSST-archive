"""tsm_system_rows

Revision ID: 0dd96901dfe1
Revises: 0586c1303f03
Create Date: 2025-10-23 09:30:24.075702

"""

from collections.abc import Sequence

from alembic_utils.pg_extension import PGExtension

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0dd96901dfe1"
down_revision: str | Sequence[str] | None = "0586c1303f03"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

tsm_extension = PGExtension(schema="public", signature="tsm_system_rows")


def upgrade() -> None:
    """Upgrade schema."""
    op.create_entity(tsm_extension)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_entity(tsm_extension)
