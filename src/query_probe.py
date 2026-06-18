"""
Active response-time probing for Power BI datasets.

Runs a representative DAX query against each configured dataset on a schedule,
times the round trip via the Execute Queries API, and produces a structured
record for the ``powerbi_query_performance`` Delta table. This is a
near-zero-cost alternative to Azure Log Analytics for response-time trending.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import requests

from src.api_client import PowerBIClient

logger = logging.getLogger(__name__)


def measure_query(
    client: PowerBIClient,
    probe_entry: dict[str, Any],
) -> dict[str, Any]:
    """Run one probe and return a structured query-performance record.

    Never raises — API/HTTP failures are captured as a ``Failed`` record so a
    single bad probe does not abort the run.

    Parameters
    ----------
    client : PowerBIClient
        Authenticated API client.
    probe_entry : dict
        Probe config with workspace_id, workspace_name, dataset_id,
        dataset_name, query_label, dax_query, and optional is_critical.

    Returns
    -------
    dict
        A row ready for ingestion into ``powerbi_query_performance``.
    """
    ts = datetime.now(timezone.utc)
    record = {
        "probe_id": str(uuid.uuid4()),
        "workspace_id": probe_entry["workspace_id"],
        "workspace_name": probe_entry.get("workspace_name"),
        "dataset_id": probe_entry["dataset_id"],
        "dataset_name": probe_entry.get("dataset_name"),
        "query_label": probe_entry.get("query_label"),
        "status": "Failed",
        "duration_ms": None,
        "row_count": None,
        "error_message": None,
        "is_critical": probe_entry.get("is_critical", False),
        "probe_timestamp": ts.isoformat(),
        "probe_date": ts.strftime("%Y-%m-%d"),
    }

    try:
        result = client.execute_dax_query(
            workspace_id=probe_entry["workspace_id"],
            dataset_id=probe_entry["dataset_id"],
            dax_query=probe_entry["dax_query"],
        )
        record["status"] = "Success"
        record["duration_ms"] = result["duration_ms"]
        record["row_count"] = result["row_count"]
        logger.info(
            "Probe '%s' on '%s': %.0f ms (%d rows).",
            record["query_label"],
            record["dataset_name"],
            result["duration_ms"],
            result["row_count"],
        )
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        record["error_message"] = f"HTTP {status}"
        logger.error(
            "Probe '%s' on '%s' failed: HTTP %s.",
            record["query_label"],
            record["dataset_name"],
            status,
        )
    except Exception as exc:  # noqa: BLE001 - record any failure, keep run alive
        record["error_message"] = str(exc)[:500]
        logger.error(
            "Probe '%s' on '%s' failed: %s",
            record["query_label"],
            record["dataset_name"],
            exc,
        )

    return record


def get_active_probes(probes_config: dict[str, Any]) -> list[dict[str, Any]]:
    """Return only the active probe entries from the config."""
    return [p for p in probes_config.get("probes", []) if p.get("is_active", True)]
