"""Initial migration

Revision ID: 056d4d155b41
Revises:
Create Date: 2024-09-20 08:52:01.691533

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "056d4d155b41"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "foldersummary",
        sa.Column("summary", sa.UnicodeText(), nullable=False),
        sa.Column("runtime", sa.Float(), nullable=True),
        sa.Column("status_short", sa.UnicodeText(), nullable=True),
        sa.Column("status_detail", sa.UnicodeText(), nullable=True),
        sa.Column("status_progress", sa.UnicodeText(), nullable=True),
        sa.Column("llm_model_prompt", sa.UnicodeText(), nullable=True),
        sa.Column("llm_model_provider", sa.UnicodeText(), nullable=True),
        sa.Column(
            "id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_deleted", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_foldersummary_runtime"), "foldersummary", ["runtime"], unique=False
    )
    op.create_index(
        op.f("ix_foldersummary_status_short"),
        "foldersummary",
        ["status_short"],
        unique=False,
    )
    op.create_table(
        "user",
        sa.Column("name", sa.UnicodeText(), nullable=False),
        sa.Column("email", sa.UnicodeText(), nullable=False),
        sa.Column("picture", sa.UnicodeText(), nullable=True),
        sa.Column("preferences", sa.UnicodeText(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("uuid", sa.UUID(), nullable=False),
        sa.Column(
            "id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_deleted", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_user_email"), "user", ["email"], unique=True)
    op.create_index(op.f("ix_user_is_active"), "user", ["is_active"], unique=False)
    op.create_index(op.f("ix_user_name"), "user", ["name"], unique=False)
    op.create_table(
        "folder",
        sa.Column("display_name", sa.UnicodeText(), nullable=False),
        sa.Column("description", sa.UnicodeText(), nullable=True),
        sa.Column("uuid", sa.UUID(), nullable=False),
        sa.Column(
            "user_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "parent_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=True,
        ),
        sa.Column(
            "id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_deleted", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["parent_id"],
            ["folder.id"],
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_folder_display_name"), "folder", ["display_name"], unique=False
    )
    op.create_table(
        "userapikey",
        sa.Column("display_name", sa.UnicodeText(), nullable=True),
        sa.Column("description", sa.UnicodeText(), nullable=True),
        sa.Column("api_key", sa.UnicodeText(), nullable=True),
        sa.Column("access_token", sa.UnicodeText(), nullable=True),
        sa.Column("expires_at", sa.DateTime(), nullable=True),
        sa.Column(
            "user_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_deleted", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_userapikey_display_name"), "userapikey", ["display_name"], unique=False
    )
    op.create_table(
        "workflowtemplate",
        sa.Column("display_name", sa.UnicodeText(), nullable=False),
        sa.Column("description", sa.UnicodeText(), nullable=True),
        sa.Column("spec_json", sa.UnicodeText(), nullable=False),
        sa.Column(
            "user_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=True,
        ),
        sa.Column(
            "id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_deleted", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_workflowtemplate_display_name"),
        "workflowtemplate",
        ["display_name"],
        unique=False,
    )
    op.create_table(
        "folderattribute",
        sa.Column(
            "folder_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_deleted", sa.Boolean(), nullable=False),
        sa.Column("key", sa.Unicode(length=255), nullable=False),
        sa.Column("value", sa.UnicodeText(), nullable=False),
        sa.Column("ontology", sa.Unicode(length=255), nullable=False),
        sa.Column("description", sa.UnicodeText(), nullable=True),
        sa.Column(
            "user_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["folder_id"],
            ["folder.id"],
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_folderattribute_key"), "folderattribute", ["key"], unique=False
    )
    op.create_index(
        op.f("ix_folderattribute_ontology"),
        "folderattribute",
        ["ontology"],
        unique=False,
    )
    op.create_table(
        "workflow",
        sa.Column("display_name", sa.UnicodeText(), nullable=False),
        sa.Column("description", sa.UnicodeText(), nullable=True),
        sa.Column("uuid", sa.UUID(), nullable=False),
        sa.Column("spec_json", sa.UnicodeText(), nullable=True),
        sa.Column(
            "user_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "folder_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=True,
        ),
        sa.Column(
            "id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_deleted", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["folder_id"],
            ["folder.id"],
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_workflow_display_name"), "workflow", ["display_name"], unique=False
    )
    op.create_table(
        "task",
        sa.Column("display_name", sa.UnicodeText(), nullable=False),
        sa.Column("description", sa.UnicodeText(), nullable=True),
        sa.Column("uuid", sa.UUID(), nullable=False),
        sa.Column("config", sa.UnicodeText(), nullable=True),
        sa.Column("status_short", sa.UnicodeText(), nullable=True),
        sa.Column("status_detail", sa.UnicodeText(), nullable=True),
        sa.Column("status_progress", sa.UnicodeText(), nullable=True),
        sa.Column("result", sa.UnicodeText(), nullable=True),
        sa.Column("runtime", sa.Float(), nullable=True),
        sa.Column("error_exception", sa.UnicodeText(), nullable=True),
        sa.Column("error_traceback", sa.UnicodeText(), nullable=True),
        sa.Column(
            "user_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "workflow_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_deleted", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user.id"],
        ),
        sa.ForeignKeyConstraint(
            ["workflow_id"],
            ["workflow.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_task_display_name"), "task", ["display_name"], unique=False
    )
    op.create_index(
        op.f("ix_task_status_short"), "task", ["status_short"], unique=False
    )
    op.create_table(
        "file",
        sa.Column("display_name", sa.UnicodeText(), nullable=True),
        sa.Column("description", sa.UnicodeText(), nullable=True),
        sa.Column("uuid", sa.UUID(), nullable=False),
        sa.Column("data_type", sa.UnicodeText(), nullable=False),
        sa.Column("filename", sa.UnicodeText(), nullable=False),
        sa.Column("filesize", sa.BigInteger(), nullable=True),
        sa.Column("extension", sa.UnicodeText(), nullable=False),
        sa.Column("magic_text", sa.UnicodeText(), nullable=True),
        sa.Column("magic_mime", sa.UnicodeText(), nullable=True),
        sa.Column("hash_md5", sa.Unicode(length=32), nullable=True),
        sa.Column("hash_sha1", sa.Unicode(length=40), nullable=True),
        sa.Column("hash_sha256", sa.Unicode(length=64), nullable=True),
        sa.Column("hash_ssdeep", sa.Unicode(length=255), nullable=True),
        sa.Column(
            "user_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "folder_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=True,
        ),
        sa.Column(
            "task_input_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=True,
        ),
        sa.Column(
            "task_output_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=True,
        ),
        sa.Column(
            "id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_deleted", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["folder_id"],
            ["folder.id"],
        ),
        sa.ForeignKeyConstraint(
            ["task_input_id"],
            ["task.id"],
        ),
        sa.ForeignKeyConstraint(
            ["task_output_id"],
            ["task.id"],
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_file_data_type"), "file", ["data_type"], unique=False)
    op.create_index(
        op.f("ix_file_display_name"), "file", ["display_name"], unique=False
    )
    op.create_index(op.f("ix_file_extension"), "file", ["extension"], unique=False)
    op.create_index(op.f("ix_file_filename"), "file", ["filename"], unique=False)
    op.create_index(op.f("ix_file_hash_md5"), "file", ["hash_md5"], unique=False)
    op.create_index(op.f("ix_file_hash_sha1"), "file", ["hash_sha1"], unique=False)
    op.create_index(op.f("ix_file_hash_sha256"), "file", ["hash_sha256"], unique=False)
    op.create_index(op.f("ix_file_hash_ssdeep"), "file", ["hash_ssdeep"], unique=False)
    op.create_index(op.f("ix_file_magic_mime"), "file", ["magic_mime"], unique=False)
    op.create_index(op.f("ix_file_magic_text"), "file", ["magic_text"], unique=False)
    op.create_table(
        "file_workflow_association_table",
        sa.Column(
            "file_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "workflow_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["file_id"],
            ["file.id"],
        ),
        sa.ForeignKeyConstraint(
            ["workflow_id"],
            ["workflow.id"],
        ),
        sa.PrimaryKeyConstraint("file_id", "workflow_id"),
    )
    op.create_table(
        "fileattribute",
        sa.Column(
            "file_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_deleted", sa.Boolean(), nullable=False),
        sa.Column("key", sa.Unicode(length=255), nullable=False),
        sa.Column("value", sa.UnicodeText(), nullable=False),
        sa.Column("ontology", sa.Unicode(length=255), nullable=False),
        sa.Column("description", sa.UnicodeText(), nullable=True),
        sa.Column(
            "user_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["file_id"],
            ["file.id"],
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_fileattribute_key"), "fileattribute", ["key"], unique=False
    )
    op.create_index(
        op.f("ix_fileattribute_ontology"), "fileattribute", ["ontology"], unique=False
    )
    op.create_table(
        "filesummary",
        sa.Column("summary", sa.UnicodeText(), nullable=False),
        sa.Column("runtime", sa.Float(), nullable=True),
        sa.Column("status_short", sa.UnicodeText(), nullable=True),
        sa.Column("status_detail", sa.UnicodeText(), nullable=True),
        sa.Column("status_progress", sa.UnicodeText(), nullable=True),
        sa.Column("llm_model_prompt", sa.UnicodeText(), nullable=True),
        sa.Column("llm_model_provider", sa.UnicodeText(), nullable=True),
        sa.Column("llm_model_name", sa.UnicodeText(), nullable=True),
        sa.Column("llm_model_config", sa.UnicodeText(), nullable=True),
        sa.Column(
            "file_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_deleted", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["file_id"],
            ["file.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_filesummary_runtime"), "filesummary", ["runtime"], unique=False
    )
    op.create_index(
        op.f("ix_filesummary_status_short"),
        "filesummary",
        ["status_short"],
        unique=False,
    )
    op.create_table(
        "filesummaryfeedback",
        sa.Column(
            "filesummary_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_deleted", sa.Boolean(), nullable=False),
        sa.Column("upvote", sa.Boolean(), nullable=False),
        sa.Column("downvote", sa.Boolean(), nullable=False),
        sa.Column("feedback_text", sa.UnicodeText(), nullable=True),
        sa.Column(
            "user_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["filesummary_id"],
            ["filesummary.id"],
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("filesummaryfeedback")
    op.drop_index(op.f("ix_filesummary_status_short"), table_name="filesummary")
    op.drop_index(op.f("ix_filesummary_runtime"), table_name="filesummary")
    op.drop_table("filesummary")
    op.drop_index(op.f("ix_fileattribute_ontology"), table_name="fileattribute")
    op.drop_index(op.f("ix_fileattribute_key"), table_name="fileattribute")
    op.drop_table("fileattribute")
    op.drop_table("file_workflow_association_table")
    op.drop_index(op.f("ix_file_magic_text"), table_name="file")
    op.drop_index(op.f("ix_file_magic_mime"), table_name="file")
    op.drop_index(op.f("ix_file_hash_ssdeep"), table_name="file")
    op.drop_index(op.f("ix_file_hash_sha256"), table_name="file")
    op.drop_index(op.f("ix_file_hash_sha1"), table_name="file")
    op.drop_index(op.f("ix_file_hash_md5"), table_name="file")
    op.drop_index(op.f("ix_file_filename"), table_name="file")
    op.drop_index(op.f("ix_file_extension"), table_name="file")
    op.drop_index(op.f("ix_file_display_name"), table_name="file")
    op.drop_index(op.f("ix_file_data_type"), table_name="file")
    op.drop_table("file")
    op.drop_index(op.f("ix_task_status_short"), table_name="task")
    op.drop_index(op.f("ix_task_display_name"), table_name="task")
    op.drop_table("task")
    op.drop_index(op.f("ix_workflow_display_name"), table_name="workflow")
    op.drop_table("workflow")
    op.drop_index(op.f("ix_folderattribute_ontology"), table_name="folderattribute")
    op.drop_index(op.f("ix_folderattribute_key"), table_name="folderattribute")
    op.drop_table("folderattribute")
    op.drop_index(
        op.f("ix_workflowtemplate_display_name"), table_name="workflowtemplate"
    )
    op.drop_table("workflowtemplate")
    op.drop_index(op.f("ix_userapikey_display_name"), table_name="userapikey")
    op.drop_table("userapikey")
    op.drop_index(op.f("ix_folder_display_name"), table_name="folder")
    op.drop_table("folder")
    op.drop_index(op.f("ix_user_name"), table_name="user")
    op.drop_index(op.f("ix_user_is_active"), table_name="user")
    op.drop_index(op.f("ix_user_email"), table_name="user")
    op.drop_table("user")
    op.drop_index(op.f("ix_foldersummary_status_short"), table_name="foldersummary")
    op.drop_index(op.f("ix_foldersummary_runtime"), table_name="foldersummary")
    op.drop_table("foldersummary")
    # ### end Alembic commands ###
