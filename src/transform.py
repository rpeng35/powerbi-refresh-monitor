"""
Data transformation logic for Power BI refresh history records.

Converts raw API JSON into structured rows suitable for Delta Lake ingestion,
including duration computation, error code extraction, and timestamp normalization.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

from dateutil import parser as dt_parser

logger = logging.getLogger(__name__)


def compute_duration_seconds(start_time: str | None, end_time: str | None) -> float | None:
    """Compute the duration between two ISO-8601 timestamps in seconds.

    Returns None if either timestamp is missing (e.g. refresh still in progress).
    """
    if not start_time or not end_time:
        return None
    try:
        st = dt_parser.isoparse(start_time)
        et = dt_parser.isoparse(end_time)
        return (et - st).total_seconds()
    except (ValueError, TypeError) as exc:
        logger.warning("Failed to parse timestamps: start=%s, end=%s — %s", start_time, end_time, exc)
        return None


def extract_error_code(service_exception_json: str | None) -> str | None:
    """Extract the errorCode field from a Power BI serviceExceptionJson string."""
    if not service_exception_json:
        return None
    try:
        return json.loads(service_exception_json).get("errorCode")
    except (json.JSONDecodeError, TypeError):
        return "PARSE_ERROR"


def filter_records_by_watermark(
    raw_records: list[dict[str, Any]],
    watermark: str | None,
) -> list[dict[str, Any]]:
    """Filter raw API records to only include those newer than the watermark.

    Parameters
    ----------
    raw_records : list[dict]
        Raw refresh records from the Power BI API.
    watermark : str | None
        ISO-8601 timestamp of the latest known record for this dataset.
        If None, all records pass through (full extraction mode).

    Returns
    -------
    list[dict]
        Records with ``startTime`` > *watermark*, or all records if
        *watermark* is None.
    """
    if not watermark:
        return raw_records

    wm_dt = dt_parser.isoparse(watermark)
    filtered = []
    for record in raw_records:
        start_time = record.get("startTime")
        if not start_time:
            filtered.append(record)
            continue
        try:
            if dt_parser.isoparse(start_time) > wm_dt:
                filtered.append(record)
        except (ValueError, TypeError):
            filtered.append(record)

    return filtered


def transform_refresh_records(
    raw_records: list[dict[str, Any]],
    dataset_id: str,
    dataset_name: str,
    workspace_id: str,
    workspace_name: str,
    is_critical: bool,
) -> list[dict[str, Any]]:
    """Transform raw Power BI API refresh records into structured rows.

    Parameters
    ----------
    raw_records : list[dict]
        Raw JSON records from the ``Get Refresh History In Group`` endpoint.
    dataset_id, dataset_name, workspace_id, workspace_name : str
        Identifiers and names from the configuration registry.
    is_critical : bool
        Whether this dataset is flagged as business-critical.

    Returns
    -------
    list[dict]
        Transformed rows ready for Delta Lake ingestion.
    """
    ingestion_ts = datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []

    for record in raw_records:
        request_id = record.get("requestId")
        if not request_id:
            logger.warning("Skipping record with missing requestId: %s", record)
            continue

        start_time = record.get("startTime")
        end_time = record.get("endTime")
        svc_exception = record.get("serviceExceptionJson")
        attempts = record.get("refreshAttempts", [])

        rows.append(
            {
                "request_id": request_id,
                "dataset_id": dataset_id,
                "dataset_name": dataset_name,
                "workspace_id": workspace_id,
                "workspace_name": workspace_name,
                "refresh_type": record.get("refreshType"),
                "status": record.get("status"),
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": compute_duration_seconds(start_time, end_time),
                "service_exception_json": svc_exception,
                "error_code": extract_error_code(svc_exception),
                "refresh_attempts_json": json.dumps(attempts) if attempts else None,
                "attempt_count": len(attempts),
                "is_critical": is_critical,
                "ingestion_timestamp": ingestion_ts.isoformat(),
                "ingestion_date": ingestion_ts.strftime("%Y-%m-%d"),
            }
        )

    return rows


def _first(record: dict[str, Any], *keys: str) -> Any:
    """Return the first present, non-None value among ``keys``.

    Power BI audit events are inconsistent about casing for a few fields
    (e.g. ``WorkspaceName`` vs ``WorkSpaceName``), so we tolerate both.
    """
    for key in keys:
        value = record.get(key)
        if value is not None:
            return value
    return None


def _activity_date(creation_time: str | None) -> str | None:
    """Derive a ``YYYY-MM-DD`` partition date from an event's CreationTime."""
    if not creation_time:
        return None
    try:
        return dt_parser.isoparse(creation_time).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        logger.warning("Failed to parse activity CreationTime: %s", creation_time)
        return None


def transform_activity_events(
    raw_events: list[dict[str, Any]],
    tracked_activities: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Normalize raw Power BI activity (audit) events into structured rows.

    Parameters
    ----------
    raw_events : list[dict]
        Raw event entities from the admin ``activityevents`` API.
    tracked_activities : list[str] | None
        If provided, only events whose ``Activity`` is in this list are kept
        (case-insensitive), e.g. ``["ViewReport", "ViewDashboard"]``.
        ``None`` keeps all event types.

    Returns
    -------
    list[dict]
        Transformed rows ready for Delta Lake ingestion.
    """
    ingestion_ts = datetime.now(timezone.utc)
    tracked = {a.lower() for a in tracked_activities} if tracked_activities else None
    rows: list[dict[str, Any]] = []

    for event in raw_events:
        event_id = event.get("Id")
        if not event_id:
            logger.warning("Skipping activity event with missing Id.")
            continue

        activity = event.get("Activity")
        if tracked is not None and (activity or "").lower() not in tracked:
            continue

        creation_time = event.get("CreationTime")
        rows.append(
            {
                "event_id": event_id,
                "creation_time": creation_time,
                "creation_date": _activity_date(creation_time),
                "activity": activity,
                "user_id": event.get("UserId"),
                "user_key": event.get("UserKey"),
                "workspace_id": _first(event, "WorkspaceId", "WorkSpaceId"),
                "workspace_name": _first(event, "WorkspaceName", "WorkSpaceName"),
                "report_id": event.get("ReportId"),
                "report_name": event.get("ReportName"),
                "report_type": event.get("ReportType"),
                "dataset_id": event.get("DatasetId"),
                "dataset_name": event.get("DatasetName"),
                "capacity_id": event.get("CapacityId"),
                "consumption_method": event.get("ConsumptionMethod"),
                "distribution_method": event.get("DistributionMethod"),
                "item_name": event.get("ItemName"),
                "object_id": event.get("ObjectId"),
                "result_status": event.get("ResultStatus"),
                "ingestion_timestamp": ingestion_ts.isoformat(),
                "ingestion_date": ingestion_ts.strftime("%Y-%m-%d"),
            }
        )

    return rows
