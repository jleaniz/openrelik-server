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

import pytest


@pytest.fixture
def importer_lib(mocker):
    """Import the aws importer with third-party deps stubbed.

    Neither boto3 nor openrelik_api_client is installed in the server's dev
    venv, so we stub them at ``sys.modules`` level before importing the
    module under test.
    """
    mock_boto3 = mocker.MagicMock()
    mock_api_client_module = mocker.MagicMock()
    mock_workflows_module = mocker.MagicMock()
    # A parent stub so ``import openrelik_api_client`` succeeds at all.
    mock_api_root = mocker.MagicMock()
    mock_api_root.api_client = mock_api_client_module
    mock_api_root.workflows = mock_workflows_module
    mocker.patch.dict(
        "sys.modules",
        {
            "boto3": mock_boto3,
            "openrelik_api_client": mock_api_root,
            "openrelik_api_client.api_client": mock_api_client_module,
            "openrelik_api_client.workflows": mock_workflows_module,
        },
    )

    from importers.aws.importer import (
        TemplateConfigError,
        _parse_template_params,
        compile_key_template,
        download_file_from_s3,
        main,
        parse_key,
        process_s3_record,
        process_sqs_message,
        render_folder_name,
        validate_folder_template,
    )

    return {
        "TemplateConfigError": TemplateConfigError,
        "_parse_template_params": _parse_template_params,
        "compile_key_template": compile_key_template,
        "download_file_from_s3": download_file_from_s3,
        "main": main,
        "parse_key": parse_key,
        "process_s3_record": process_s3_record,
        "process_sqs_message": process_sqs_message,
        "render_folder_name": render_folder_name,
        "validate_folder_template": validate_folder_template,
    }


def _make_s3_record(
    bucket="fcicollectors",
    key="users/mytestCase/data/A_new_file_20261212.ize",
    size=100,
    event_name="ObjectCreated:Put",
):
    return {
        "eventName": event_name,
        "s3": {
            "bucket": {"name": bucket},
            "object": {"key": key, "size": size},
        },
    }


def _make_sqs_message(records, receipt_handle="rh-1", sns_wrapped=False):
    inner = {"Records": records}
    body = {"Message": json.dumps(inner)} if sns_wrapped else inner
    return {"Body": json.dumps(body), "ReceiptHandle": receipt_handle}


# ---------------------------------------------------------------------------
# compile_key_template
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "template,key,expected",
    [
        (
            "users/{case}/data/{filename}",
            "users/mytestCase/data/foo.ize",
            {"case": "mytestCase", "filename": "foo.ize"},
        ),
        (
            "{case}/{filename}",
            "mytestCase/foo.ize",
            {"case": "mytestCase", "filename": "foo.ize"},
        ),
        (
            "tenants/{org}/cases/{case}/uploads/{filename}",
            "tenants/acme/cases/mytestCase/uploads/foo.ize",
            {"org": "acme", "case": "mytestCase", "filename": "foo.ize"},
        ),
        (
            "collectors/{source}/{case}/{filename}",
            "collectors/edr/mytestCase/foo.ize",
            {"source": "edr", "case": "mytestCase", "filename": "foo.ize"},
        ),
        (
            # {filename} is greedy — nested paths under the last segment are kept.
            "users/{case}/data/{filename}",
            "users/case1/data/sub/dir/file.txt",
            {"case": "case1", "filename": "sub/dir/file.txt"},
        ),
    ],
)
def test_compile_key_template_matches(importer_lib, template, key, expected):
    pattern, _ = importer_lib["compile_key_template"](template)
    match = pattern.match(key)
    assert match is not None
    assert match.groupdict() == expected


@pytest.mark.parametrize(
    "template,bad_key",
    [
        ("users/{case}/data/{filename}", "uploads/case1/data/file.txt"),
        ("users/{case}/data/{filename}", "users/case1/other/file.txt"),
        ("users/{case}/data/{filename}", "users/case1/data"),
        ("{case}/{filename}", ""),
    ],
)
def test_compile_key_template_rejects_non_matching_keys(
    importer_lib, template, bad_key
):
    pattern, _ = importer_lib["compile_key_template"](template)
    assert pattern.match(bad_key) is None


@pytest.mark.parametrize(
    "bad_template",
    [
        "",  # empty
        "users/{case}/{case}/{filename}",  # duplicate placeholder
        "users/{case}/data",  # missing {filename}
        "users/data/{filename}",  # missing {case}
        "users/{filename}/{case}",  # {filename} not last
        "users//{case}/{filename}",  # empty segment
        "prefix-{case}/{filename}",  # partial-segment placeholder
    ],
)
def test_compile_key_template_rejects_bad_templates(importer_lib, bad_template):
    with pytest.raises(importer_lib["TemplateConfigError"]):
        importer_lib["compile_key_template"](bad_template)


# ---------------------------------------------------------------------------
# validate_folder_template
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "folder_template,key_placeholders",
    [
        ("{case}", ["case", "filename"]),
        ("{org}-{case}", ["org", "case", "filename"]),
        ("prefix-{case}-suffix", ["case", "filename"]),
        ("{source}_{case}_v1", ["source", "case", "filename"]),
    ],
)
def test_validate_folder_template_accepts(
    importer_lib, folder_template, key_placeholders
):
    importer_lib["validate_folder_template"](folder_template, key_placeholders)


@pytest.mark.parametrize(
    "folder_template,key_placeholders",
    [
        ("", ["case", "filename"]),
        ("{case}/{filename}", ["case", "filename"]),
        ("{team}/{case}", ["case", "filename"]),
        ("{team}-{case}", ["case", "filename"]),
    ],
    ids=["empty", "references_filename", "contains_slash", "unknown_placeholder"],
)
def test_validate_folder_template_rejects(
    importer_lib, folder_template, key_placeholders
):
    with pytest.raises(importer_lib["TemplateConfigError"]):
        importer_lib["validate_folder_template"](folder_template, key_placeholders)


# ---------------------------------------------------------------------------
# _parse_template_params
# ---------------------------------------------------------------------------


def test_parse_template_params_empty_returns_empty_dict(importer_lib):
    assert importer_lib["_parse_template_params"]("") == {}


def test_parse_template_params_valid_object(importer_lib):
    assert importer_lib["_parse_template_params"](
        '{"param_1": "value"}'
    ) == {"param_1": "value"}


@pytest.mark.parametrize("bad_raw", ["not-json", "[1, 2, 3]", '"scalar"'])
def test_parse_template_params_rejects_bad_values(importer_lib, bad_raw):
    with pytest.raises(importer_lib["TemplateConfigError"]):
        importer_lib["_parse_template_params"](bad_raw)


# ---------------------------------------------------------------------------
# parse_key + render_folder_name (module-level default templates)
# ---------------------------------------------------------------------------


def test_parse_key_valid(importer_lib):
    captured = importer_lib["parse_key"](
        "users/mytestCase/data/A_new_file_20261212.ize"
    )
    assert captured == {
        "case": "mytestCase",
        "filename": "A_new_file_20261212.ize",
    }


def test_parse_key_raises_on_bad_layout(importer_lib):
    with pytest.raises(ValueError):
        importer_lib["parse_key"]("uploads/case1/data/file.txt")


def test_render_folder_name_default_template(importer_lib):
    assert (
        importer_lib["render_folder_name"]({"case": "mytestCase", "filename": "x"})
        == "mytestCase"
    )


# ---------------------------------------------------------------------------
# download_file_from_s3
# ---------------------------------------------------------------------------


def test_download_file_from_s3(importer_lib, mocker):
    mock_s3 = mocker.MagicMock()
    importer_lib["download_file_from_s3"](
        mock_s3, "my-bucket", "my-object", "/path/to/output"
    )
    mock_s3.download_file.assert_called_once_with(
        "my-bucket", "my-object", "/path/to/output"
    )


# ---------------------------------------------------------------------------
# process_s3_record
# ---------------------------------------------------------------------------


def _patch_successful_dependencies(mocker, folder_path="/folder/path", folder_id=7):
    mock_get_or_create = mocker.patch(
        "importers.aws.importer.get_or_create_root_folder"
    )
    mock_folder = mocker.MagicMock()
    mock_folder.path = folder_path
    mock_folder.id = folder_id
    mock_get_or_create.return_value = mock_folder

    mock_download = mocker.patch("importers.aws.importer.download_file_from_s3")
    mock_create = mocker.patch("importers.aws.importer.create_file_record")
    mock_file_db = mocker.MagicMock()
    mock_file_db.id = 123
    mock_create.return_value = mock_file_db
    mock_hashes = mocker.patch("importers.aws.importer.generate_hashes")

    return {
        "get_or_create": mock_get_or_create,
        "folder": mock_folder,
        "download": mock_download,
        "create": mock_create,
        "file_db": mock_file_db,
        "hashes": mock_hashes,
    }


def test_process_s3_record_success(importer_lib, mocker):
    patches = _patch_successful_dependencies(mocker)
    mock_db = mocker.MagicMock()

    record = _make_s3_record(
        key="users/mytestCase/data/A_new_file_20261212.ize", size=100
    )

    importer_lib["process_s3_record"](mocker.MagicMock(), record, mock_db)

    patches["get_or_create"].assert_called_once()
    args, _ = patches["get_or_create"].call_args
    assert args[0] is mock_db
    assert args[1] == "mytestCase"

    patches["download"].assert_called_once()
    download_args = patches["download"].call_args.args
    assert download_args[3].startswith("/folder/path/")
    assert download_args[3].endswith(".ize")

    patches["create"].assert_called_once()
    patches["hashes"].assert_called_once_with(123)


def test_process_s3_record_url_encoded_key_is_decoded(importer_lib, mocker):
    """'+' and '%20' should both decode to spaces before parsing."""
    patches = _patch_successful_dependencies(mocker)

    record = _make_s3_record(key="users/case+one/data/my+file%20name.txt")
    importer_lib["process_s3_record"](mocker.MagicMock(), record, mocker.MagicMock())

    args, _ = patches["get_or_create"].call_args
    assert args[1] == "case one"
    # create_file_record(db, filename, file_uuid, file_extension, folder_id, user_id)
    assert patches["create"].call_args.args[1] == "my file name.txt"


def test_process_s3_record_skips_directory_marker(importer_lib, mocker):
    mock_get_or_create = mocker.patch(
        "importers.aws.importer.get_or_create_root_folder"
    )

    importer_lib["process_s3_record"](
        mocker.MagicMock(),
        _make_s3_record(key="users/case1/data/"),
        mocker.MagicMock(),
    )

    mock_get_or_create.assert_not_called()


def test_process_s3_record_skips_bad_layout(importer_lib, mocker):
    mock_get_or_create = mocker.patch(
        "importers.aws.importer.get_or_create_root_folder"
    )

    importer_lib["process_s3_record"](
        mocker.MagicMock(),
        _make_s3_record(key="uploads/case1/data/file.txt"),
        mocker.MagicMock(),
    )

    mock_get_or_create.assert_not_called()


def test_process_s3_record_download_error_does_not_create_file(importer_lib, mocker):
    patches = _patch_successful_dependencies(mocker)
    patches["download"].side_effect = Exception("boom")

    importer_lib["process_s3_record"](
        mocker.MagicMock(),
        _make_s3_record(key="users/case1/data/file.txt"),
        mocker.MagicMock(),
    )

    patches["create"].assert_not_called()


def test_process_s3_record_skips_hashing_for_large_files(importer_lib, mocker):
    patches = _patch_successful_dependencies(mocker)

    # size > HASH_SIZE_LIMIT (10 MB)
    record = _make_s3_record(
        key="users/case1/data/big.bin", size=20 * 1024 * 1024
    )
    importer_lib["process_s3_record"](mocker.MagicMock(), record, mocker.MagicMock())

    patches["hashes"].assert_not_called()


def test_process_s3_record_auto_creates_folder_for_new_case(importer_lib, mocker):
    """The importer must not require the case folder to exist before the event."""
    patches = _patch_successful_dependencies(mocker)

    importer_lib["process_s3_record"](
        mocker.MagicMock(),
        _make_s3_record(key="users/brand-new-case/data/file.txt"),
        mocker.MagicMock(),
    )

    args, _ = patches["get_or_create"].call_args
    assert args[1] == "brand-new-case"
    patches["create"].assert_called_once()


def test_process_s3_record_no_workflow_when_template_id_unset(
    importer_lib, mocker
):
    """With AWS_IMPORT_TEMPLATE_ID unset (default), no workflow is created."""
    patches = _patch_successful_dependencies(mocker)
    mock_get_api = mocker.patch("importers.aws.importer._get_workflows_api")

    importer_lib["process_s3_record"](
        mocker.MagicMock(),
        _make_s3_record(key="users/case1/data/file.txt"),
        mocker.MagicMock(),
    )

    # File still imported, but no workflow machinery touched.
    patches["create"].assert_called_once()
    mock_get_api.assert_not_called()


def test_process_s3_record_runs_workflow_when_template_id_set(
    importer_lib, mocker
):
    patches = _patch_successful_dependencies(mocker)

    from importers.aws import importer as aws_importer

    mocker.patch.object(aws_importer, "AWS_IMPORT_TEMPLATE_ID", "7")
    mocker.patch.object(
        aws_importer, "AWS_IMPORT_TEMPLATE_PARAMS", {"my_param_0": "value"}
    )

    mock_workflows_api = mocker.MagicMock()
    mock_workflows_api.create_workflow.return_value = 42
    mocker.patch.object(
        aws_importer, "_get_workflows_api", return_value=mock_workflows_api
    )

    importer_lib["process_s3_record"](
        mocker.MagicMock(),
        _make_s3_record(key="users/case1/data/file.txt"),
        mocker.MagicMock(),
    )

    patches["create"].assert_called_once()
    mock_workflows_api.create_workflow.assert_called_once_with(
        folder_id=patches["folder"].id,
        file_ids=[patches["file_db"].id],
        template_id=7,
        template_params={"my_param_0": "value"},
    )
    mock_workflows_api.run_workflow.assert_called_once_with(
        folder_id=patches["folder"].id, workflow_id=42
    )


def test_process_s3_record_workflow_error_does_not_fail_import(
    importer_lib, mocker
):
    """API-side workflow failure must not swallow the successful file import."""
    patches = _patch_successful_dependencies(mocker)

    from importers.aws import importer as aws_importer

    mocker.patch.object(aws_importer, "AWS_IMPORT_TEMPLATE_ID", "7")
    mock_workflows_api = mocker.MagicMock()
    mock_workflows_api.create_workflow.side_effect = Exception("api down")
    mocker.patch.object(
        aws_importer, "_get_workflows_api", return_value=mock_workflows_api
    )

    importer_lib["process_s3_record"](
        mocker.MagicMock(),
        _make_s3_record(key="users/case1/data/file.txt"),
        mocker.MagicMock(),
    )

    # Import still succeeded (file created, hashes triggered).
    patches["create"].assert_called_once()
    patches["hashes"].assert_called_once()


def test_process_s3_record_logs_http_error_body(importer_lib, mocker, caplog):
    """HTTPError from the API client should be logged with the response body."""
    from requests import HTTPError

    patches = _patch_successful_dependencies(mocker)

    from importers.aws import importer as aws_importer

    mocker.patch.object(aws_importer, "AWS_IMPORT_TEMPLATE_ID", "7")

    mock_response = mocker.MagicMock()
    mock_response.text = '{"detail":"Workflow template 9999 not found"}'
    err = HTTPError("404 Client Error", response=mock_response)

    mock_workflows_api = mocker.MagicMock()
    mock_workflows_api.create_workflow.side_effect = err
    mocker.patch.object(
        aws_importer, "_get_workflows_api", return_value=mock_workflows_api
    )

    with caplog.at_level("ERROR", logger="importers.aws.importer"):
        importer_lib["process_s3_record"](
            mocker.MagicMock(),
            _make_s3_record(key="users/case1/data/file.txt"),
            mocker.MagicMock(),
        )

    # Import itself still succeeded.
    patches["create"].assert_called_once()
    # The API's response body made it into the log message.
    assert any(
        "Workflow template 9999 not found" in rec.message for rec in caplog.records
    ), "expected HTTPError body to be logged"


def test_process_s3_record_no_workflow_run_when_create_returns_none(
    importer_lib, mocker
):
    """create_workflow returning None (non-200) must skip run_workflow."""
    _patch_successful_dependencies(mocker)

    from importers.aws import importer as aws_importer

    mocker.patch.object(aws_importer, "AWS_IMPORT_TEMPLATE_ID", "7")
    mock_workflows_api = mocker.MagicMock()
    mock_workflows_api.create_workflow.return_value = None
    mocker.patch.object(
        aws_importer, "_get_workflows_api", return_value=mock_workflows_api
    )

    importer_lib["process_s3_record"](
        mocker.MagicMock(),
        _make_s3_record(key="users/case1/data/file.txt"),
        mocker.MagicMock(),
    )

    mock_workflows_api.run_workflow.assert_not_called()


def test_process_s3_record_uses_custom_folder_template(importer_lib, mocker):
    """Override AWS_FOLDER_TEMPLATE to verify the rendered folder name is used."""
    patches = _patch_successful_dependencies(mocker)

    # Patch the module-level template; parse_key still runs the default key
    # template so we inject extra placeholders via a different key template.
    from importers.aws import importer as aws_importer

    key_pattern, _ = aws_importer.compile_key_template(
        "tenants/{org}/cases/{case}/uploads/{filename}"
    )
    mocker.patch.object(aws_importer, "KEY_PATTERN", key_pattern)
    mocker.patch.object(aws_importer, "AWS_FOLDER_TEMPLATE", "{org}-{case}")

    importer_lib["process_s3_record"](
        mocker.MagicMock(),
        _make_s3_record(key="tenants/acme/cases/mytestCase/uploads/foo.ize"),
        mocker.MagicMock(),
    )

    args, _ = patches["get_or_create"].call_args
    assert args[1] == "acme-mytestCase"


# ---------------------------------------------------------------------------
# process_sqs_message
# ---------------------------------------------------------------------------


def test_process_sqs_message_direct_s3_event(importer_lib, mocker):
    mock_handler = mocker.patch("importers.aws.importer.process_s3_record")

    record = _make_s3_record()
    importer_lib["process_sqs_message"](
        mocker.MagicMock(), _make_sqs_message([record]), mocker.MagicMock()
    )

    mock_handler.assert_called_once()
    assert mock_handler.call_args.args[1] == record


def test_process_sqs_message_sns_wrapped(importer_lib, mocker):
    mock_handler = mocker.patch("importers.aws.importer.process_s3_record")

    record = _make_s3_record()
    importer_lib["process_sqs_message"](
        mocker.MagicMock(),
        _make_sqs_message([record], sns_wrapped=True),
        mocker.MagicMock(),
    )

    mock_handler.assert_called_once()
    assert mock_handler.call_args.args[1] == record


def test_process_sqs_message_no_records(importer_lib, mocker):
    """A message with no 'Records' (e.g. s3:TestEvent) must be a no-op."""
    mock_handler = mocker.patch("importers.aws.importer.process_s3_record")

    message = {"Body": json.dumps({"Event": "s3:TestEvent"}), "ReceiptHandle": "rh"}
    importer_lib["process_sqs_message"](
        mocker.MagicMock(), message, mocker.MagicMock()
    )

    mock_handler.assert_not_called()


def test_process_sqs_message_skips_non_object_created(importer_lib, mocker):
    mock_handler = mocker.patch("importers.aws.importer.process_s3_record")
    record = _make_s3_record(event_name="ObjectRemoved:Delete")
    importer_lib["process_sqs_message"](
        mocker.MagicMock(), _make_sqs_message([record]), mocker.MagicMock()
    )
    mock_handler.assert_not_called()


def test_process_sqs_message_multiple_records(importer_lib, mocker):
    mock_handler = mocker.patch("importers.aws.importer.process_s3_record")

    records = [
        _make_s3_record(key="users/case1/data/a.txt"),
        _make_s3_record(
            key="users/case1/data/b.txt", event_name="ObjectRemoved:Delete"
        ),
        _make_s3_record(key="users/case1/data/c.txt"),
    ]
    importer_lib["process_sqs_message"](
        mocker.MagicMock(), _make_sqs_message(records), mocker.MagicMock()
    )

    assert mock_handler.call_count == 2


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def test_main_no_robot_user(importer_lib, mocker):
    mocker.patch("importers.aws.importer.ROBOT_ACCOUNT_USER_ID", None)
    mock_boto = mocker.patch("importers.aws.importer.boto3.client")

    importer_lib["main"]()

    mock_boto.assert_not_called()


def test_main_no_queue_url(importer_lib, mocker):
    mocker.patch("importers.aws.importer.ROBOT_ACCOUNT_USER_ID", "1")
    mocker.patch("importers.aws.importer.SQS_QUEUE_URL", None)
    mock_boto = mocker.patch("importers.aws.importer.boto3.client")

    importer_lib["main"]()

    mock_boto.assert_not_called()


def test_main_processes_message_and_deletes(importer_lib, mocker):
    mocker.patch("importers.aws.importer.ROBOT_ACCOUNT_USER_ID", "1")
    mocker.patch(
        "importers.aws.importer.SQS_QUEUE_URL", "https://sqs.example/queue"
    )

    mock_sqs = mocker.MagicMock()
    mock_s3 = mocker.MagicMock()
    mocker.patch(
        "importers.aws.importer.boto3.client",
        side_effect=lambda s, **_: {"sqs": mock_sqs, "s3": mock_s3}[s],
    )
    mocker.patch("importers.aws.importer.database")

    message = _make_sqs_message([_make_s3_record()], receipt_handle="rh-42")
    mock_sqs.receive_message.side_effect = [
        {"Messages": [message]},
        KeyboardInterrupt(),
    ]

    mock_process = mocker.patch("importers.aws.importer.process_sqs_message")

    with pytest.raises(KeyboardInterrupt):
        importer_lib["main"]()

    mock_process.assert_called_once()
    mock_sqs.delete_message.assert_called_once_with(
        QueueUrl="https://sqs.example/queue", ReceiptHandle="rh-42"
    )


def test_main_processing_error_does_not_delete(importer_lib, mocker):
    mocker.patch("importers.aws.importer.ROBOT_ACCOUNT_USER_ID", "1")
    mocker.patch(
        "importers.aws.importer.SQS_QUEUE_URL", "https://sqs.example/queue"
    )

    mock_sqs = mocker.MagicMock()
    mock_s3 = mocker.MagicMock()
    mocker.patch(
        "importers.aws.importer.boto3.client",
        side_effect=lambda s, **_: {"sqs": mock_sqs, "s3": mock_s3}[s],
    )
    mocker.patch("importers.aws.importer.database")

    message = _make_sqs_message([_make_s3_record()])
    mock_sqs.receive_message.side_effect = [
        {"Messages": [message]},
        KeyboardInterrupt(),
    ]

    mocker.patch(
        "importers.aws.importer.process_sqs_message",
        side_effect=Exception("processing failed"),
    )

    with pytest.raises(KeyboardInterrupt):
        importer_lib["main"]()

    mock_sqs.delete_message.assert_not_called()


def test_main_receive_error_retries(importer_lib, mocker):
    mocker.patch("importers.aws.importer.ROBOT_ACCOUNT_USER_ID", "1")
    mocker.patch(
        "importers.aws.importer.SQS_QUEUE_URL", "https://sqs.example/queue"
    )

    mock_sqs = mocker.MagicMock()
    mock_s3 = mocker.MagicMock()
    mocker.patch(
        "importers.aws.importer.boto3.client",
        side_effect=lambda s, **_: {"sqs": mock_sqs, "s3": mock_s3}[s],
    )
    mocker.patch("importers.aws.importer.database")
    mock_sleep = mocker.patch("importers.aws.importer.time.sleep")

    mock_sqs.receive_message.side_effect = [
        Exception("transient"),
        KeyboardInterrupt(),
    ]

    with pytest.raises(KeyboardInterrupt):
        importer_lib["main"]()

    mock_sleep.assert_called_once_with(5)


def test_main_empty_receive_keeps_polling(importer_lib, mocker):
    mocker.patch("importers.aws.importer.ROBOT_ACCOUNT_USER_ID", "1")
    mocker.patch(
        "importers.aws.importer.SQS_QUEUE_URL", "https://sqs.example/queue"
    )

    mock_sqs = mocker.MagicMock()
    mock_s3 = mocker.MagicMock()
    mocker.patch(
        "importers.aws.importer.boto3.client",
        side_effect=lambda s, **_: {"sqs": mock_sqs, "s3": mock_s3}[s],
    )
    mocker.patch("importers.aws.importer.database")

    mock_process = mocker.patch("importers.aws.importer.process_sqs_message")
    mock_sqs.receive_message.side_effect = [
        {"Messages": []},
        KeyboardInterrupt(),
    ]

    with pytest.raises(KeyboardInterrupt):
        importer_lib["main"]()

    mock_process.assert_not_called()
    mock_sqs.delete_message.assert_not_called()
