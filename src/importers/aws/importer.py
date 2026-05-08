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
import json
import logging
import os
import re
import string
import time
import uuid
from typing import Any, Dict, List, Tuple
from urllib.parse import unquote_plus

import boto3
from sqlalchemy.orm import Session

from datastores.sql import database
from datastores.sql.crud.user import get_user_from_db
from datastores.sql.models.user import User
from importers.file_utils import create_file_record, get_or_create_root_folder
from lib import workflow_utils
from lib.file_hashes import generate_hashes
from lib.workflow_utils import TemplateNotFoundError

AWS_REGION = os.environ.get("AWS_REGION")
SQS_QUEUE_URL = os.environ.get("AWS_SQS_QUEUE_URL")
ROBOT_ACCOUNT_USER_ID = os.environ.get("ROBOT_ACCOUNT_USER_ID")
HASH_SIZE_LIMIT = 10485760  # 10MB

# Optional: after each successful file import, create and run a workflow
# against the imported file using the configured template. Leave
# AWS_IMPORT_TEMPLATE_ID unset to disable (importer just ingests files).
AWS_IMPORT_TEMPLATE_ID = os.environ.get("AWS_IMPORT_TEMPLATE_ID")
AWS_IMPORT_TEMPLATE_PARAMS_RAW = os.environ.get("AWS_IMPORT_TEMPLATE_PARAMS", "")

# How the importer interprets S3 keys. Both are operator-configurable.
#
# AWS_KEY_TEMPLATE describes the S3 key layout using `{placeholder}` segments
# separated by literal `/` segments. `{case}` and `{filename}` are required;
# any other placeholders (e.g. `{org}`) are captured and available for
# rendering into the folder name.
#
# AWS_FOLDER_TEMPLATE describes the openrelik root-folder name, rendered from
# the placeholders captured by the key template. Defaults to `{case}`, which
# preserves single-tenant behavior. Set to e.g. `{org}-{case}` to keep
# multi-tenant cases from colliding.
DEFAULT_KEY_TEMPLATE = "users/{case}/data/{filename}"
DEFAULT_FOLDER_TEMPLATE = "{case}"
AWS_KEY_TEMPLATE = os.environ.get("AWS_KEY_TEMPLATE", DEFAULT_KEY_TEMPLATE)
AWS_FOLDER_TEMPLATE = os.environ.get("AWS_FOLDER_TEMPLATE", DEFAULT_FOLDER_TEMPLATE)

# SQS long-poll wait (seconds). Max allowed by SQS is 20.
SQS_WAIT_TIME_SECONDS = 20
# Max SQS messages per receive call (SQS cap is 10).
SQS_MAX_MESSAGES = 10
# Back-off when the receive call itself fails, to avoid busy-looping.
RECEIVE_ERROR_BACKOFF_SECONDS = 5

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


_PLACEHOLDER_RE = re.compile(r"^\{([a-zA-Z_][a-zA-Z0-9_]*)\}$")


class TemplateConfigError(ValueError):
    """Raised when AWS_KEY_TEMPLATE / AWS_FOLDER_TEMPLATE are misconfigured."""


def compile_key_template(template: str) -> Tuple[re.Pattern, List[str]]:
    """Compile an AWS_KEY_TEMPLATE value into a matching regex.

    Each `/`-delimited segment of the template must be either:
      * a literal string (escaped), or
      * exactly one placeholder of the form `{name}`.

    The special `{filename}` placeholder is treated greedily (matches the rest
    of the key, including any remaining `/`) and must therefore appear as the
    final segment. All other placeholders match a single non-empty segment.

    Args:
        template: The raw template string.

    Returns:
        A tuple of (compiled_regex, ordered_placeholder_names).

    Raises:
        TemplateConfigError: If the template is malformed or does not capture
            both `{case}` and `{filename}`.
    """
    if not template:
        raise TemplateConfigError("AWS_KEY_TEMPLATE must not be empty.")

    segments = template.split("/")
    parts: List[str] = []
    names: List[str] = []
    for idx, segment in enumerate(segments):
        if not segment:
            raise TemplateConfigError(
                f"AWS_KEY_TEMPLATE has an empty segment: {template!r}"
            )
        match = _PLACEHOLDER_RE.match(segment)
        if match:
            name = match.group(1)
            if name in names:
                raise TemplateConfigError(
                    f"AWS_KEY_TEMPLATE placeholder {{{name}}} appears more than once."
                )
            names.append(name)
            if name == "filename":
                if idx != len(segments) - 1:
                    raise TemplateConfigError(
                        "{filename} must be the last segment of AWS_KEY_TEMPLATE."
                    )
                parts.append(r"(?P<filename>.+)")
            else:
                parts.append(rf"(?P<{name}>[^/]+)")
        else:
            # Reject partial-segment placeholders up front; they aren't supported.
            if "{" in segment or "}" in segment:
                raise TemplateConfigError(
                    f"AWS_KEY_TEMPLATE segment {segment!r} mixes literal text and "
                    "placeholders; each segment must be either a full literal or "
                    "a standalone {placeholder}."
                )
            parts.append(re.escape(segment))

    missing = {"case", "filename"} - set(names)
    if missing:
        raise TemplateConfigError(
            "AWS_KEY_TEMPLATE must capture both {case} and {filename}; missing: "
            + ", ".join(sorted(missing))
        )

    return re.compile("^" + "/".join(parts) + "$"), names


def validate_folder_template(template: str, key_placeholders: List[str]) -> None:
    """Validate AWS_FOLDER_TEMPLATE at startup.

    Args:
        template: The raw folder-template string.
        key_placeholders: Placeholder names captured by AWS_KEY_TEMPLATE.

    Raises:
        TemplateConfigError: If the folder template references unknown
            placeholders, references `{filename}`, or contains `/` (which
            would imply nested subfolders, unsupported here).
    """
    if not template:
        raise TemplateConfigError("AWS_FOLDER_TEMPLATE must not be empty.")
    if "/" in template:
        raise TemplateConfigError(
            "AWS_FOLDER_TEMPLATE must not contain '/'; only flat root folders are supported."
        )

    referenced = {
        field_name
        for _, field_name, _, _ in string.Formatter().parse(template)
        if field_name
    }
    if "filename" in referenced:
        raise TemplateConfigError(
            "AWS_FOLDER_TEMPLATE must not reference {filename}; "
            "that would create one folder per file."
        )
    unknown = referenced - set(key_placeholders)
    if unknown:
        raise TemplateConfigError(
            "AWS_FOLDER_TEMPLATE references placeholders not captured by "
            "AWS_KEY_TEMPLATE: " + ", ".join(sorted(unknown))
        )


# Compile templates at import time so misconfiguration fails the container
# startup loudly instead of silently skipping every message at runtime.
KEY_PATTERN, KEY_PLACEHOLDERS = compile_key_template(AWS_KEY_TEMPLATE)
validate_folder_template(AWS_FOLDER_TEMPLATE, KEY_PLACEHOLDERS)


def _parse_template_params(raw: str) -> Dict[str, Any]:
    """Parse AWS_IMPORT_TEMPLATE_PARAMS at startup, or return {} if unset."""
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise TemplateConfigError(
            f"AWS_IMPORT_TEMPLATE_PARAMS is not valid JSON: {e}"
        )
    if not isinstance(parsed, dict):
        raise TemplateConfigError(
            "AWS_IMPORT_TEMPLATE_PARAMS must decode to a JSON object."
        )
    return parsed


AWS_IMPORT_TEMPLATE_PARAMS = _parse_template_params(AWS_IMPORT_TEMPLATE_PARAMS_RAW)


def parse_key(object_key: str) -> Dict[str, str]:
    """Match ``object_key`` against the configured key template.

    Args:
        object_key: The URL-decoded S3 object key.

    Returns:
        A mapping of placeholder name to captured value, including at least
        ``case`` and ``filename``.

    Raises:
        ValueError: If the key does not match the template or any captured
            value is empty.
    """
    match = KEY_PATTERN.match(object_key)
    if not match:
        raise ValueError(
            f"Key '{object_key}' does not match AWS_KEY_TEMPLATE "
            f"{AWS_KEY_TEMPLATE!r}."
        )
    captured = match.groupdict()
    empty = [name for name, value in captured.items() if not value]
    if empty:
        raise ValueError(
            f"Key '{object_key}' has empty captures for: {', '.join(sorted(empty))}"
        )
    return captured


def render_folder_name(captured: Dict[str, str]) -> str:
    """Render the configured folder template using captured placeholders.

    Args:
        captured: Placeholder values from ``parse_key``.

    Returns:
        The rendered folder display name.

    Raises:
        ValueError: If the rendered name is empty after stripping whitespace.
    """
    rendered = AWS_FOLDER_TEMPLATE.format(**captured).strip()
    if not rendered:
        raise ValueError(
            "Rendered folder name is empty; check AWS_FOLDER_TEMPLATE."
        )
    return rendered


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
        captured = parse_key(object_key)
        folder_name = render_folder_name(captured)
    except ValueError as e:
        logger.error(f"Skipping object with unexpected key layout: {e}")
        return

    filename = captured["filename"]
    _, file_extension = os.path.splitext(filename)
    file_uuid = uuid.uuid4()
    output_filename = f"{file_uuid.hex}{file_extension}"

    # Resolve or auto-create the per-case root folder owned by the robot user.
    folder = get_or_create_root_folder(db, folder_name, ROBOT_ACCOUNT_USER_ID)

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
    if AWS_IMPORT_TEMPLATE_ID:
        try:
            _run_template_workflow(
                db,
                folder_id=folder.id,
                file_id=new_file_db.id,
                user=robot_user,
            )
        except TemplateNotFoundError as e:
            logger.error(
                f"Workflow auto-run failed for file {new_file_db.id}: {e}"
            )
        except Exception as e:
            logger.exception(
                f"Workflow auto-run failed for file {new_file_db.id}: {e}"
            )

    logger.info(f"Successfully processed s3://{bucket_name}/{object_key}")


def _run_template_workflow(
    db: Session, *, folder_id: int, file_id: int, user: User
) -> None:
    """Create a workflow from the configured template and dispatch it, in-process.

    Args:
        db: Database session.
        folder_id: The openrelik folder the imported file lives in. The
            workflow will be created as a subfolder underneath.
        file_id: The id of the newly imported file to run against.
        user: The user under which the workflow is created and run.
    """
    workflow = workflow_utils.create_workflow_from_template(
        db,
        folder_id=folder_id,
        file_ids=[file_id],
        template_id=int(AWS_IMPORT_TEMPLATE_ID),
        template_params=AWS_IMPORT_TEMPLATE_PARAMS,
        user=user,
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


def _extract_s3_records(message: Dict[str, Any]) -> List[Dict[str, Any]]:
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
        r
        for r in records
        if str(r.get("eventName", "")).startswith("ObjectCreated:")
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
    logger.info(
        f"Starting to poll SQS queue {SQS_QUEUE_URL} with key template "
        f"{AWS_KEY_TEMPLATE!r} and folder template "
        f"{AWS_FOLDER_TEMPLATE!r}.{template_note}"
    )

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
                sqs.delete_message(
                    QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle
                )
            except Exception as e:
                logger.exception(f"Error deleting SQS message: {e}")


if __name__ == "__main__":
    main()
