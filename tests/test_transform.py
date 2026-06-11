"""Unit tests for the transform module."""

import json
from datetime import datetime, timezone

import pytest

from src.transform import (
    compute_duration_seconds,
    extract_error_code,
    filter_records_by_watermark,
    transform_activity_events,
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


class TestFilterRecordsByWatermark:
    def test_none_watermark_returns_all(self):
        records = [
            {"requestId": "r1", "startTime": "2024-06-01T08:00:00Z"},
            {"requestId": "r2", "startTime": "2024-06-02T08:00:00Z"},
        ]
        result = filter_records_by_watermark(records, None)
        assert len(result) == 2

    def test_empty_watermark_returns_all(self):
        records = [{"requestId": "r1", "startTime": "2024-06-01T08:00:00Z"}]
        result = filter_records_by_watermark(records, "")
        assert len(result) == 1

    def test_filters_old_records(self):
        records = [
            {"requestId": "old", "startTime": "2024-06-01T08:00:00Z"},
            {"requestId": "new", "startTime": "2024-06-03T08:00:00Z"},
        ]
        result = filter_records_by_watermark(records, "2024-06-02T00:00:00Z")
        assert len(result) == 1
        assert result[0]["requestId"] == "new"

    def test_exact_watermark_excluded(self):
        records = [
            {"requestId": "exact", "startTime": "2024-06-02T00:00:00Z"},
        ]
        result = filter_records_by_watermark(records, "2024-06-02T00:00:00Z")
        assert len(result) == 0

    def test_record_without_start_time_included(self):
        records = [
            {"requestId": "no-time"},
            {"requestId": "old", "startTime": "2024-06-01T00:00:00Z"},
        ]
        result = filter_records_by_watermark(records, "2024-06-02T00:00:00Z")
        assert len(result) == 1
        assert result[0]["requestId"] == "no-time"

    def test_all_records_newer(self):
        records = [
            {"requestId": "r1", "startTime": "2024-06-05T00:00:00Z"},
            {"requestId": "r2", "startTime": "2024-06-06T00:00:00Z"},
        ]
        result = filter_records_by_watermark(records, "2024-06-01T00:00:00Z")
        assert len(result) == 2

    def test_all_records_older(self):
        records = [
            {"requestId": "r1", "startTime": "2024-06-01T00:00:00Z"},
            {"requestId": "r2", "startTime": "2024-06-02T00:00:00Z"},
        ]
        result = filter_records_by_watermark(records, "2024-06-10T00:00:00Z")
        assert len(result) == 0

    def test_empty_records(self):
        result = filter_records_by_watermark([], "2024-06-01T00:00:00Z")
        assert result == []

    def test_unparseable_start_time_included(self):
        records = [
            {"requestId": "bad", "startTime": "not-a-date"},
            {"requestId": "good", "startTime": "2024-06-05T00:00:00Z"},
        ]
        result = filter_records_by_watermark(records, "2024-06-01T00:00:00Z")
        assert len(result) == 2


class TestTransformActivityEvents:
    SAMPLE = [
        {
            "Id": "evt-1",
            "CreationTime": "2024-06-01T13:45:00Z",
            "Activity": "ViewReport",
            "UserId": "alice@contoso.com",
            "WorkspaceId": "ws-1",
            "WorkSpaceName": "Finance Reports",
            "ReportId": "rpt-1",
            "ReportName": "Monthly Revenue",
            "DatasetId": "ds-1",
            "ResultStatus": "Succeeded",
        },
        {
            "Id": "evt-2",
            "CreationTime": "2024-06-01T14:00:00Z",
            "Activity": "CreateReport",
            "UserId": "bob@contoso.com",
        },
    ]

    def test_maps_core_fields(self):
        rows = transform_activity_events(self.SAMPLE)
        assert len(rows) == 2
        first = rows[0]
        assert first["event_id"] == "evt-1"
        assert first["activity"] == "ViewReport"
        assert first["user_id"] == "alice@contoso.com"
        assert first["report_name"] == "Monthly Revenue"
        assert first["creation_date"] == "2024-06-01"
        # Tolerates the WorkSpaceName casing variant.
        assert first["workspace_name"] == "Finance Reports"
        assert first["workspace_id"] == "ws-1"

    def test_filters_by_tracked_activities(self):
        rows = transform_activity_events(self.SAMPLE, tracked_activities=["ViewReport"])
        assert len(rows) == 1
        assert rows[0]["event_id"] == "evt-1"

    def test_tracked_activities_case_insensitive(self):
        rows = transform_activity_events(self.SAMPLE, tracked_activities=["viewreport"])
        assert len(rows) == 1

    def test_skips_events_without_id(self):
        rows = transform_activity_events([{"Activity": "ViewReport"}])
        assert rows == []

    def test_unparseable_creation_time_yields_null_date(self):
        rows = transform_activity_events(
            [{"Id": "x", "CreationTime": "not-a-date", "Activity": "ViewReport"}]
        )
        assert rows[0]["creation_date"] is None

    def test_missing_optional_fields_are_none(self):
        rows = transform_activity_events(self.SAMPLE)
        assert rows[1]["report_name"] is None
        assert rows[1]["workspace_id"] is None
