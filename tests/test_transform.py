"""Unit tests for the transform module."""

import json
from datetime import datetime, timezone

import pytest

from src.transform import (
    compute_duration_seconds,
    extract_error_code,
    transform_refresh_records,
)


class TestComputeDurationSeconds:
    def test_valid_timestamps(self):
        result = compute_duration_seconds(
            "2024-01-15T09:00:00.000Z",
            "2024-01-15T09:05:30.000Z",
        )
        assert result == 330.0

    def test_sub_second_precision(self):
        result = compute_duration_seconds(
            "2024-01-15T09:00:00.000Z",
            "2024-01-15T09:00:00.500Z",
        )
        assert result == 0.5

    def test_none_start(self):
        assert compute_duration_seconds(None, "2024-01-15T09:00:00Z") is None

    def test_none_end(self):
        assert compute_duration_seconds("2024-01-15T09:00:00Z", None) is None

    def test_both_none(self):
        assert compute_duration_seconds(None, None) is None

    def test_empty_strings(self):
        assert compute_duration_seconds("", "") is None


class TestExtractErrorCode:
    def test_valid_json(self):
        exc = '{"errorCode":"ModelRefreshFailed_CredentialsNotSpecified"}'
        assert extract_error_code(exc) == "ModelRefreshFailed_CredentialsNotSpecified"

    def test_no_error_code_key(self):
        assert extract_error_code('{"message":"something"}') is None

    def test_none_input(self):
        assert extract_error_code(None) is None

    def test_empty_string(self):
        assert extract_error_code("") is None

    def test_invalid_json(self):
        assert extract_error_code("not-json{") == "PARSE_ERROR"


class TestTransformRefreshRecords:
    SAMPLE_RECORD = {
        "requestId": "abc-123",
        "refreshType": "Scheduled",
        "startTime": "2024-06-01T08:00:00.000Z",
        "endTime": "2024-06-01T08:05:00.000Z",
        "status": "Completed",
        "serviceExceptionJson": None,
        "refreshAttempts": [
            {
                "attemptId": 1,
                "startTime": "2024-06-01T08:00:00.000Z",
                "endTime": "2024-06-01T08:05:00.000Z",
                "type": "Data",
            }
        ],
    }

    def test_single_completed_record(self):
        rows = transform_refresh_records(
            raw_records=[self.SAMPLE_RECORD],
            dataset_id="ds-1",
            dataset_name="Test Dataset",
            workspace_id="ws-1",
            workspace_name="Test Workspace",
            is_critical=True,
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["request_id"] == "abc-123"
        assert row["dataset_id"] == "ds-1"
        assert row["dataset_name"] == "Test Dataset"
        assert row["workspace_id"] == "ws-1"
        assert row["workspace_name"] == "Test Workspace"
        assert row["refresh_type"] == "Scheduled"
        assert row["status"] == "Completed"
        assert row["duration_seconds"] == 300.0
        assert row["error_code"] is None
        assert row["attempt_count"] == 1
        assert row["is_critical"] is True

    def test_in_progress_record(self):
        record = {
            "requestId": "in-prog-1",
            "refreshType": "ViaApi",
            "startTime": "2024-06-01T10:00:00.000Z",
            "status": "Unknown",
        }
        rows = transform_refresh_records(
            raw_records=[record],
            dataset_id="ds-1",
            dataset_name="Test",
            workspace_id="ws-1",
            workspace_name="WS",
            is_critical=False,
        )
        assert len(rows) == 1
        assert rows[0]["status"] == "Unknown"
        assert rows[0]["end_time"] is None
        assert rows[0]["duration_seconds"] is None

    def test_failed_record_with_error(self):
        record = {
            "requestId": "fail-1",
            "refreshType": "Scheduled",
            "startTime": "2024-06-01T08:00:00.000Z",
            "endTime": "2024-06-01T08:02:00.000Z",
            "status": "Failed",
            "serviceExceptionJson": '{"errorCode":"ModelRefreshFailed_CredentialsNotSpecified"}',
            "refreshAttempts": [],
        }
        rows = transform_refresh_records(
            raw_records=[record],
            dataset_id="ds-1",
            dataset_name="Test",
            workspace_id="ws-1",
            workspace_name="WS",
            is_critical=True,
        )
        assert len(rows) == 1
        assert rows[0]["status"] == "Failed"
        assert rows[0]["error_code"] == "ModelRefreshFailed_CredentialsNotSpecified"
        assert rows[0]["duration_seconds"] == 120.0

    def test_skips_record_without_request_id(self):
        record = {
            "refreshType": "Scheduled",
            "startTime": "2024-06-01T08:00:00.000Z",
            "status": "Completed",
        }
        rows = transform_refresh_records(
            raw_records=[record],
            dataset_id="ds-1",
            dataset_name="Test",
            workspace_id="ws-1",
            workspace_name="WS",
            is_critical=False,
        )
        assert len(rows) == 0

    def test_multiple_records(self):
        records = [
            {**self.SAMPLE_RECORD, "requestId": f"req-{i}"}
            for i in range(5)
        ]
        rows = transform_refresh_records(
            raw_records=records,
            dataset_id="ds-1",
            dataset_name="Test",
            workspace_id="ws-1",
            workspace_name="WS",
            is_critical=False,
        )
        assert len(rows) == 5
        assert {r["request_id"] for r in rows} == {f"req-{i}" for i in range(5)}

    def test_ingestion_metadata_present(self):
        rows = transform_refresh_records(
            raw_records=[self.SAMPLE_RECORD],
            dataset_id="ds-1",
            dataset_name="Test",
            workspace_id="ws-1",
            workspace_name="WS",
            is_critical=False,
        )
        row = rows[0]
        assert row["ingestion_timestamp"] is not None
        assert row["ingestion_date"] is not None
