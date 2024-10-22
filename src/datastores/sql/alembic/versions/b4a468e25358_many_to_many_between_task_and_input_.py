"""Many-to-many between task and input files

Revision ID: b4a468e25358
Revises: 05c14b5e27ab
Create Date: 2024-10-22 18:07:49.926450

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b4a468e25358'
down_revision: Union[str, None] = '05c14b5e27ab'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('file_task_input_association',
    sa.Column('task_id', sa.BigInteger().with_variant(sa.Integer(), 'sqlite'), nullable=False),
    sa.Column('file_id', sa.BigInteger().with_variant(sa.Integer(), 'sqlite'), nullable=False),
    sa.ForeignKeyConstraint(['file_id'], ['file.id'], ),
    sa.ForeignKeyConstraint(['task_id'], ['task.id'], ),
    sa.PrimaryKeyConstraint('task_id', 'file_id')
    )
    op.drop_constraint('file_task_input_id_fkey', 'file', type_='foreignkey')
    op.drop_column('file', 'task_input_id')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('file', sa.Column('task_input_id', sa.BIGINT(), autoincrement=False, nullable=True))
    op.create_foreign_key('file_task_input_id_fkey', 'file', 'task', ['task_input_id'], ['id'])
    op.drop_table('file_task_input_association')
    # ### end Alembic commands ###
