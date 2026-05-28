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
