# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""AWS S3/SQS importer: polls SQS for object-create events and ingests files."""

import json
import logging
import os
import time
import uuid
from typing import Any, Dict
from urllib.parse import unquote_plus

import boto3
from sqlalchemy.orm import Session

from datastores.sql import database
from datastores.sql.crud.user import get_user_from_db
from datastores.sql.models.user import User
from importers.importer_utils import (
    create_file_record,
    get_or_create_root_folder,
    get_or_create_subfolder,
)
from lib import workflow_utils
from lib.file_hashes import generate_hashes
from lib.workflow_utils import TemplateNotFoundError

# AWS connection.
AWS_REGION = os.environ.get("AWS_REGION")
SQS_QUEUE_URL = os.environ.get("AWS_SQS_QUEUE_URL")

# Import behavior.
HASH_SIZE_LIMIT = 10485760  # 10MB
ROBOT_ACCOUNT_USER_ID = os.environ.get("ROBOT_ACCOUNT_USER_ID")

# Optional workflow auto-run: after each successful file import, create and
# run a workflow against the imported file using the configured template.
# Leave AWS_IMPORT_TEMPLATE_ID unset to disable (importer just ingests files).
AWS_IMPORT_TEMPLATE_ID = os.environ.get("AWS_IMPORT_TEMPLATE_ID")
AWS_IMPORT_TEMPLATE_PARAMS_RAW = os.environ.get("AWS_IMPORT_TEMPLATE_PARAMS", "")

# SQS polling tunables.
# Back-off when the receive call itself fails, to avoid busy-looping.
RECEIVE_ERROR_BACKOFF_SECONDS = 5
# Max SQS messages per receive call (SQS cap is 10).
SQS_MAX_MESSAGES = 10
# SQS long-poll wait (seconds). Max allowed by SQS is 20.
SQS_WAIT_TIME_SECONDS = 20

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _parse_template_params(raw: str) -> Dict[str, Any]:
    """Parse AWS_IMPORT_TEMPLATE_PARAMS at startup, or return {} if unset."""
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"AWS_IMPORT_TEMPLATE_PARAMS is not valid JSON: {e}")
    if not isinstance(parsed, dict):
        raise ValueError("AWS_IMPORT_TEMPLATE_PARAMS must decode to a JSON object.")
    return parsed


AWS_IMPORT_TEMPLATE_PARAMS = _parse_template_params(AWS_IMPORT_TEMPLATE_PARAMS_RAW)


def parse_key(object_key: str) -> tuple[list[str], str]:
    """Split an S3 key into (folder path segments, filename).

    The key's directory structure is mirrored into the OpenRelik folder
    tree. The key must contain at least one ``/`` — keys with no prefix
    are rejected because the importer has no folder to place them under.

    Examples:
        ``root/abc/data/file.zip`` -> ``(["root", "abc", "data"], "file.zip")``
        ``uploads/file.txt``         -> ``(["uploads"], "file.txt")``

    Args:
        object_key: The URL-decoded S3 object key.

    Returns:
        A 2-tuple of (path_parts, filename).

    Raises:
        ValueError: If the key has no ``/`` (no folder segment) or any
            segment is empty.
    """
    parts = object_key.split("/")
    if len(parts) < 2:
        raise ValueError(
            f"Key {object_key!r} has no folder prefix; expected at least "
            "one '/' separator."
        )
    *path_parts, filename = parts
    if not filename or any(not p for p in path_parts):
        raise ValueError(f"Key {object_key!r} contains an empty path segment.")
    return path_parts, filename


def download_file_from_s3(
    s3_client: Any, bucket_name: str, object_key: str, output_path: str
) -> None:
    """Downloads a file from S3.

    Args:
        s3_client: A boto3 S3 client.
        bucket_name: Name of the S3 bucket.
        object_key: S3 object key.
        output_path: Local path to save the downloaded file.
    """
    s3_client.download_file(bucket_name, object_key, output_path)
    logger.info(f"Downloaded s3://{bucket_name}/{object_key} to {output_path}")


def process_s3_record(
    s3_client: Any,
    record: Dict[str, Any],
    db: Session,
    robot_user: User,
) -> None:
    """Processes a single S3 event record from an SQS message.

    Args:
        s3_client: A boto3 S3 client.
        record: The S3 event record.
        db: Database session.
        robot_user: The user under which imports and auto-run workflows are
            attributed.
    """
    # S3 keys in notifications are URL-encoded, and '+' represents a space.
    raw_key = record["s3"]["object"]["key"]
    object_key = unquote_plus(raw_key)
    bucket_name = record["s3"]["bucket"]["name"]
    object_size = int(record["s3"]["object"].get("size", 0))

    logger.info(f"Processing S3 object: s3://{bucket_name}/{object_key}")

    # Directory markers still come through; nothing to import.
    if object_key.endswith("/"):
        logger.info("S3 directory marker, nothing to import.")
        return

    try:
        path_parts, filename = parse_key(object_key)
    except ValueError as e:
        logger.error(f"Skipping object with unexpected key layout: {e}")
        return

    _, file_extension = os.path.splitext(filename)
    file_uuid = uuid.uuid4()
    output_filename = f"{file_uuid.hex}{file_extension}"

    # Mirror the S3 directory path into the robot user's folder tree.
    folder = get_or_create_root_folder(db, path_parts[0], ROBOT_ACCOUNT_USER_ID)
    for segment in path_parts[1:]:
        folder = get_or_create_subfolder(db, folder.id, segment, ROBOT_ACCOUNT_USER_ID)

    output_path = os.path.join(folder.path, output_filename)

    try:
        download_file_from_s3(s3_client, bucket_name, object_key, output_path)
    except Exception as e:  # boto3 raises many client-specific exceptions.
        logger.error(f"Error downloading file from S3: {e}")
        return

    new_file_db = create_file_record(
        db,
        filename,
        file_uuid,
        file_extension,
        folder.id,
        ROBOT_ACCOUNT_USER_ID,
    )

    # Only hash small files inline; larger files should be hashed by a
    # background job.
    if object_size < HASH_SIZE_LIMIT:
        generate_hashes(new_file_db.id)

    # If a workflow template is configured, kick off a run against the newly
    # imported file. Failures here must not bubble up: the file is already
    # on disk and in the DB, so the user can re-run manually via the UI.
    workflow_auto_run_ok = True
    if AWS_IMPORT_TEMPLATE_ID:
        try:
            _run_template_workflow(
                db,
                folder_id=folder.id,
                file_id=new_file_db.id,
                display_name=f"{filename}.workflow",
                user=robot_user,
            )
        except TemplateNotFoundError as e:
            logger.error(
                f"Workflow template {AWS_IMPORT_TEMPLATE_ID} not found for file {new_file_db.id}: {e}"
            )
            workflow_auto_run_ok = False
        except Exception as e:
            logger.exception(f"Workflow auto-run failed for file {new_file_db.id}: {e}")
            workflow_auto_run_ok = False

    if workflow_auto_run_ok:
        logger.info(f"Successfully processed s3://{bucket_name}/{object_key}")
    else:
        logger.warning(
            f"Processed s3://{bucket_name}/{object_key} but workflow auto-run failed."
        )


def _run_template_workflow(
    db: Session,
    *,
    folder_id: int,
    file_id: int,
    display_name: str,
    user: User,
) -> None:
    """Create a workflow from the configured template and dispatch it, in-process.

    Args:
        db: Database session.
        folder_id: The openrelik folder the imported file lives in. The
            workflow will be created as a subfolder underneath.
        file_id: The id of the newly imported file to run against.
        display_name: Display name for the new workflow and its results
            subfolder.
        user: The user under which the workflow is created and run.
    """
    workflow = workflow_utils.create_workflow_from_template(
        db,
        folder_id=folder_id,
        file_ids=[file_id],
        template_id=int(AWS_IMPORT_TEMPLATE_ID),
        template_params=AWS_IMPORT_TEMPLATE_PARAMS,
        user=user,
        display_name=display_name,
    )
    workflow_utils.run_workflow(
        db,
        workflow=workflow,
        workflow_spec=json.loads(workflow.spec_json),
        user=user,
    )
    logger.info(
        f"Started workflow {workflow.id} from template "
        f"{AWS_IMPORT_TEMPLATE_ID} for file {file_id}"
    )


def _extract_s3_records(message: Dict[str, Any]) -> list[Dict[str, Any]]:
    """Extracts S3 event records from an SQS message body.

    SQS can deliver S3 events directly or wrapped inside an SNS notification.
    This handles both shapes and returns only records for object-create events.

    Args:
        message: An SQS message dict.

    Returns:
        A list of S3 event records for ObjectCreated events.
    """
    body = json.loads(message["Body"])
    # SNS-wrapped notifications put the S3 payload inside the "Message" field.
    if isinstance(body, dict) and "Message" in body and "Records" not in body:
        body = json.loads(body["Message"])

    records = body.get("Records") if isinstance(body, dict) else None
    if not records:
        return []

    return [
        r for r in records if str(r.get("eventName", "")).startswith("ObjectCreated:")
    ]


def process_sqs_message(
    s3_client: Any,
    message: Dict[str, Any],
    db: Session,
    robot_user: User,
) -> None:
    """Processes a single SQS message, which may contain multiple S3 records.

    Args:
        s3_client: A boto3 S3 client.
        message: The SQS message dict.
        db: Database session.
        robot_user: The user under which imports and auto-run workflows are
            attributed.
    """
    records = _extract_s3_records(message)
    if not records:
        logger.info("SQS message contains no ObjectCreated records, skipping.")
        return

    for record in records:
        process_s3_record(s3_client, record, db, robot_user)


def main() -> None:
    """Poll the configured SQS queue and process incoming S3 events.

    Messages are only deleted from the queue after successful processing so
    that transient failures result in redelivery (subject to the queue's
    redrive policy).
    """
    if not ROBOT_ACCOUNT_USER_ID:
        logger.error("ROBOT_ACCOUNT_USER_ID environment variable is not set.")
        return
    if not SQS_QUEUE_URL:
        logger.error("AWS_SQS_QUEUE_URL environment variable is not set.")
        return

    # Resolve the robot user once at startup. Bail loudly if it's missing so
    # misconfiguration is obvious instead of silently tanking every message.
    with database.SessionLocal() as db:
        robot_user = get_user_from_db(db, int(ROBOT_ACCOUNT_USER_ID))
    if robot_user is None:
        logger.error(
            f"ROBOT_ACCOUNT_USER_ID={ROBOT_ACCOUNT_USER_ID!r} does not match "
            "any user in the database."
        )
        return

    sqs = boto3.client("sqs", region_name=AWS_REGION)
    s3 = boto3.client("s3", region_name=AWS_REGION)

    template_note = (
        f" Workflow auto-run enabled (template_id={AWS_IMPORT_TEMPLATE_ID})."
        if AWS_IMPORT_TEMPLATE_ID
        else " Workflow auto-run disabled."
    )
    logger.info(f"Starting to poll SQS queue {SQS_QUEUE_URL}.{template_note}")

    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=SQS_MAX_MESSAGES,
                WaitTimeSeconds=SQS_WAIT_TIME_SECONDS,
            )
        except Exception as e:  # boto3 exceptions vary; back off and retry.
            logger.exception(f"Error receiving SQS messages: {e}")
            time.sleep(RECEIVE_ERROR_BACKOFF_SECONDS)
            continue

        messages = response.get("Messages", [])
        if not messages:
            continue

        for message in messages:
            receipt_handle = message.get("ReceiptHandle")
            try:
                with database.SessionLocal() as db:
                    process_sqs_message(s3, message, db, robot_user)
            except Exception as e:
                logger.exception(f"Error processing SQS message: {e}")
                # Don't delete — let the queue redeliver after visibility timeout.
                continue

            try:
                sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            except Exception as e:
                logger.exception(f"Error deleting SQS message: {e}")


if __name__ == "__main__":
    main()
