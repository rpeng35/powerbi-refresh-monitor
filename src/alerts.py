"""
Alerting framework for Power BI refresh monitoring.

Provides post-ingestion checks that detect:
  - Critical refresh failures
  - Duration anomalies (exceeding rolling average threshold)
  - Stale data (no successful refresh within SLA window)

Alerts are dispatched via configurable channels (logging, webhooks, email).
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger(__name__)


@dataclass
class Alert:
    """Represents a single alert event."""

    severity: str  # "critical", "warning", "info"
    alert_type: str  # "refresh_failure", "duration_anomaly", "stale_data"
    dataset_name: str
    workspace_name: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class AlertDispatcher:
    """Dispatches alerts to configured channels."""

    def __init__(self, teams_webhook_url: str | None = None):
        self._teams_webhook_url = teams_webhook_url

    def dispatch(self, alert: Alert) -> None:
        """Send an alert to all configured channels."""
        self._log_alert(alert)
        if self._teams_webhook_url:
            self._send_teams(alert)

    def _log_alert(self, alert: Alert) -> None:
        log_level = logging.CRITICAL if alert.severity == "critical" else logging.WARNING
        logger.log(
            log_level,
            "[%s] %s | %s/%s: %s",
            alert.severity.upper(),
            alert.alert_type,
            alert.workspace_name,
            alert.dataset_name,
            alert.message,
        )

    def _send_teams(self, alert: Alert) -> None:
        severity_color = {
            "critical": "attention",
            "warning": "warning",
            "info": "good",
        }
        color = severity_color.get(alert.severity, "default")

        # Adaptive Card payload for Microsoft Teams incoming webhook
        card = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "contentUrl": None,
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": [
                            {
                                "type": "TextBlock",
                                "size": "Large",
                                "weight": "Bolder",
                                "text": f"Power BI Refresh Alert \u2014 {alert.alert_type.replace('_', ' ').title()}",
                                "style": "heading",
                                "color": color,
                            },
                            {
                                "type": "FactSet",
                                "facts": [
                                    {"title": "Severity", "value": alert.severity.upper()},
                                    {"title": "Dataset", "value": alert.dataset_name},
                                    {"title": "Workspace", "value": alert.workspace_name},
                                    {"title": "Time (UTC)", "value": alert.timestamp},
                                ],
                            },
                            {
                                "type": "TextBlock",
                                "text": alert.message,
                                "wrap": True,
                            },
                        ],
                    },
                }
            ],
        }

        try:
            resp = requests.post(self._teams_webhook_url, json=card, timeout=10)
            resp.raise_for_status()
            logger.info("Teams alert sent successfully.")
        except requests.RequestException as exc:
            logger.error("Failed to send Teams alert: %s", exc)


def check_refresh_failures(
    spark,
    table_name: str,
    dispatcher: AlertDispatcher,
) -> list[Alert]:
    """Detect critical refresh failures from the latest ingestion.

    Parameters
    ----------
    spark : SparkSession
    table_name : str
        Fully qualified Delta table name.
    dispatcher : AlertDispatcher

    Returns
    -------
    list[Alert]
        Alerts generated (also dispatched via the dispatcher).
    """
    query = f"""
        SELECT dataset_name, workspace_name, error_code, service_exception_json,
               start_time, is_critical
        FROM {table_name}
        WHERE status = 'Failed'
          AND ingestion_date = current_date()
        ORDER BY is_critical DESC, start_time DESC
    """
    failures = spark.sql(query).collect()
    alerts: list[Alert] = []

    for row in failures:
        severity = "critical" if row.is_critical else "warning"
        alert = Alert(
            severity=severity,
            alert_type="refresh_failure",
            dataset_name=row.dataset_name or "Unknown",
            workspace_name=row.workspace_name or "Unknown",
            message=f"Refresh failed with error: {row.error_code or 'Unknown'}",
            details={
                "error_code": row.error_code,
                "service_exception": row.service_exception_json,
                "start_time": str(row.start_time),
            },
        )
        dispatcher.dispatch(alert)
        alerts.append(alert)

    if not failures:
        logger.info("No refresh failures detected in today's ingestion.")

    return alerts


def check_duration_anomalies(
    spark,
    table_name: str,
    dispatcher: AlertDispatcher,
    threshold_multiplier: float = 1.5,
    rolling_window_days: int = 7,
) -> list[Alert]:
    """Detect refreshes whose duration exceeds a rolling average threshold.

    An anomaly is flagged when:
        duration > threshold_multiplier × (7-day rolling average duration)

    Parameters
    ----------
    spark : SparkSession
    table_name : str
    dispatcher : AlertDispatcher
    threshold_multiplier : float
        Factor above the rolling average that triggers an alert (default: 1.5×).
    rolling_window_days : int
        Number of days for the rolling average window (default: 7).

    Returns
    -------
    list[Alert]
    """
    query = f"""
        WITH recent AS (
            SELECT
                dataset_id,
                dataset_name,
                workspace_name,
                duration_seconds,
                is_critical,
                start_time,
                ingestion_date,
                AVG(duration_seconds) OVER (
                    PARTITION BY dataset_id
                    ORDER BY start_time
                    ROWS BETWEEN {rolling_window_days * 4} PRECEDING AND 1 PRECEDING
                ) AS rolling_avg_duration
            FROM {table_name}
            WHERE status = 'Completed'
              AND duration_seconds IS NOT NULL
              AND start_time >= current_date() - INTERVAL {rolling_window_days + 1} DAYS
        )
        SELECT *
        FROM recent
        WHERE ingestion_date = current_date()
          AND rolling_avg_duration IS NOT NULL
          AND duration_seconds > {threshold_multiplier} * rolling_avg_duration
    """
    anomalies = spark.sql(query).collect()
    alerts: list[Alert] = []

    for row in anomalies:
        severity = "warning"
        ratio = row.duration_seconds / row.rolling_avg_duration if row.rolling_avg_duration else 0
        alert = Alert(
            severity=severity,
            alert_type="duration_anomaly",
            dataset_name=row.dataset_name or "Unknown",
            workspace_name=row.workspace_name or "Unknown",
            message=(
                f"Refresh took {row.duration_seconds:.0f}s "
                f"({ratio:.1f}x the {rolling_window_days}-day rolling avg of "
                f"{row.rolling_avg_duration:.0f}s)."
            ),
            details={
                "duration_seconds": row.duration_seconds,
                "rolling_avg_seconds": row.rolling_avg_duration,
                "ratio": round(ratio, 2),
            },
        )
        dispatcher.dispatch(alert)
        alerts.append(alert)

    if not anomalies:
        logger.info("No duration anomalies detected.")

    return alerts


def check_stale_data(
    spark,
    table_name: str,
    dispatcher: AlertDispatcher,
    staleness_threshold_hours: int = 26,
) -> list[Alert]:
    """Detect critical datasets with no successful refresh within the SLA window.

    Parameters
    ----------
    spark : SparkSession
    table_name : str
    dispatcher : AlertDispatcher
    staleness_threshold_hours : int
        Hours since the last successful refresh before an alert fires (default: 26).

    Returns
    -------
    list[Alert]
    """
    query = f"""
        WITH last_success AS (
            SELECT
                dataset_id,
                dataset_name,
                workspace_name,
                MAX(end_time) AS last_successful_refresh
            FROM {table_name}
            WHERE status = 'Completed'
              AND is_critical = true
            GROUP BY dataset_id, dataset_name, workspace_name
        )
        SELECT
            dataset_name,
            workspace_name,
            last_successful_refresh,
            TIMESTAMPDIFF(HOUR, last_successful_refresh, current_timestamp()) AS hours_since_refresh
        FROM last_success
        WHERE TIMESTAMPDIFF(HOUR, last_successful_refresh, current_timestamp()) > {staleness_threshold_hours}
    """
    stale = spark.sql(query).collect()
    alerts: list[Alert] = []

    for row in stale:
        alert = Alert(
            severity="critical",
            alert_type="stale_data",
            dataset_name=row.dataset_name or "Unknown",
            workspace_name=row.workspace_name or "Unknown",
            message=(
                f"No successful refresh in {row.hours_since_refresh} hours "
                f"(SLA threshold: {staleness_threshold_hours}h). "
                f"Last success: {row.last_successful_refresh}."
            ),
            details={
                "hours_since_refresh": row.hours_since_refresh,
                "last_successful_refresh": str(row.last_successful_refresh),
            },
        )
        dispatcher.dispatch(alert)
        alerts.append(alert)

    if not stale:
        logger.info("All critical datasets are within freshness SLA.")

    return alerts
