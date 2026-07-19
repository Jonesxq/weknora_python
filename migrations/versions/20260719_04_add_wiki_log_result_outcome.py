"""为 Wiki 操作日志增加批次终态快照

Revision ID: 20260719_04
Revises: 20260718_03
Create Date: 2026-07-19
"""

from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "20260719_04"
down_revision: str | None = "20260718_03"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "wiki_log_entries",
        sa.Column(
            "result_outcome",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("wiki_log_entries", "result_outcome")
