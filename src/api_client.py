"""
Power BI REST API client for fetching refresh histories.

Handles HTTP session management, retry logic with exponential backoff,
and per-dataset refresh history retrieval.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

BASE_URL = "https://api.powerbi.com/v1.0/myorg"

# HTTP status codes that warrant automatic retry
RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]

# Response headers that carry throttling / quota info across the various
# Microsoft and RFC conventions. Matched case-insensitively when logging.
_RATE_LIMIT_HEADER_PREFIXES = ("ratelimit-", "x-ratelimit-", "x-ms-")
_RATE_LIMIT_HEADER_EXACT = ("retry-after",)


class RateLimitError(Exception):
    """Raised when an endpoint is throttled (HTTP 429) after retries are exhausted.

    Carries ``retry_after`` (seconds, if the API supplied it) so callers can
    decide to back off and resume later rather than silently skipping data.
    """

    def __init__(self, message: str, retry_after: int | None = None):
        super().__init__(message)
        self.retry_after = retry_after


def _rate_limit_headers(headers: Any) -> dict[str, str]:
    """Return only the throttling-relevant response headers (for diagnostics)."""
    found: dict[str, str] = {}
    for name, value in headers.items():
        low = name.lower()
        if low in _RATE_LIMIT_HEADER_EXACT or low.startswith(_RATE_LIMIT_HEADER_PREFIXES):
            found[name] = value
    return found


def _parse_retry_after(headers: Any) -> int | None:
    """Parse the ``Retry-After`` header (delta-seconds form) into an int."""
    raw = headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _raise_if_throttled(response: requests.Response, context: str) -> None:
    """Convert a 429 response into a :class:`RateLimitError`, logging the headers.

    Called after the session's automatic retries have already been exhausted, so
    reaching here means the endpoint is still throttled and the caller should
    stop rather than lose data.
    """
    if response.status_code != 429:
        return
    retry_after = _parse_retry_after(response.headers)
    logger.error(
        "Throttled (HTTP 429) on %s after retries. Retry-After=%ss. Rate-limit headers: %s",
        context,
        retry_after,
        _rate_limit_headers(response.headers),
    )
    raise RateLimitError(
        f"Rate limited (429) on {context}; Retry-After={retry_after}s",
        retry_after=retry_after,
    )


def _count_rows(payload: dict[str, Any]) -> int:
    """Count rows returned by an Execute Queries response (0 if none/malformed)."""
    try:
        return len(payload["results"][0]["tables"][0]["rows"])
    except (KeyError, IndexError, TypeError):
        return 0


class PowerBIClient:
    """REST client for the Power BI API with built-in retry logic."""

    def __init__(
        self,
        access_token: str,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        request_timeout: int = 30,
    ):
        self._access_token = access_token
        self._request_timeout = request_timeout
        self._session = self._create_session(max_retries, backoff_factor)

    def _create_session(self, max_retries: int, backoff_factor: float) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=RETRYABLE_STATUS_CODES,
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        return session

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }

    def get_refresh_history(
        self,
        workspace_id: str,
        dataset_id: str,
        top: int = 60,
    ) -> list[dict[str, Any]]:
        """Fetch refresh history for a single dataset in a workspace.

        Parameters
        ----------
        workspace_id : str
            Power BI workspace (group) ID.
        dataset_id : str
            Power BI dataset (semantic model) ID.
        top : int
            Maximum number of refresh entries to return (API default: 60).

        Returns
        -------
        list[dict]
            List of refresh record dicts from the API response.

        Raises
        ------
        requests.HTTPError
            On non-retryable HTTP errors (401, 403, etc.).
        """
        url = f"{BASE_URL}/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
        params = {"$top": top}

        response = self._session.get(
            url,
            headers=self._headers,
            params=params,
            timeout=self._request_timeout,
        )
        response.raise_for_status()
        return response.json().get("value", [])

    def get_refresh_history_safe(
        self,
        workspace_id: str,
        dataset_id: str,
        dataset_name: str,
        top: int = 60,
    ) -> list[dict[str, Any]]:
        """Fetch refresh history with graceful error handling.

        Logs warnings for expected failure modes (404, 403) and returns
        an empty list instead of raising.  Unexpected errors are re-raised.

        Parameters
        ----------
        workspace_id : str
            Power BI workspace (group) ID.
        dataset_id : str
            Power BI dataset (semantic model) ID.
        dataset_name : str
            Human-readable name for logging.
        top : int
            Maximum number of refresh entries to return.

        Returns
        -------
        list[dict]
        """
        try:
            return self.get_refresh_history(workspace_id, dataset_id, top)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 404:
                logger.warning(
                    "Dataset '%s' (%s) not found in workspace %s — it may have been deleted. Skipping.",
                    dataset_name,
                    dataset_id,
                    workspace_id,
                )
                return []
            if status == 403:
                logger.error(
                    "Access denied for dataset '%s' (%s) in workspace %s. "
                    "Ensure the Service Principal has Member access.",
                    dataset_name,
                    dataset_id,
                    workspace_id,
                )
                return []
            raise

    def get_datasets_in_workspace(
        self,
        workspace_id: str,
    ) -> list[dict[str, Any]]:
        """Fetch all datasets in a workspace.

        Parameters
        ----------
        workspace_id : str
            Power BI workspace (group) ID.

        Returns
        -------
        list[dict]
            List of dataset dicts from the API response.

        Raises
        ------
        requests.HTTPError
            On non-retryable HTTP errors.
        """
        url = f"{BASE_URL}/groups/{workspace_id}/datasets"

        response = self._session.get(
            url,
            headers=self._headers,
            timeout=self._request_timeout,
        )
        response.raise_for_status()
        return response.json().get("value", [])

    def get_datasets_in_workspace_safe(
        self,
        workspace_id: str,
        workspace_name: str,
    ) -> list[dict[str, Any]]:
        """Fetch all datasets in a workspace with graceful error handling.

        Logs warnings for expected failure modes (404, 403) and returns
        an empty list instead of raising.

        Parameters
        ----------
        workspace_id : str
            Power BI workspace (group) ID.
        workspace_name : str
            Human-readable name for logging.

        Returns
        -------
        list[dict]
        """
        try:
            return self.get_datasets_in_workspace(workspace_id)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 404:
                logger.warning(
                    "Workspace '%s' (%s) not found — it may have been deleted. Skipping.",
                    workspace_name,
                    workspace_id,
                )
                return []
            if status == 403:
                logger.error(
                    "Access denied for workspace '%s' (%s). "
                    "Ensure the Service Principal has workspace access.",
                    workspace_name,
                    workspace_id,
                )
                return []
            raise

    def execute_dax_query(
        self,
        workspace_id: str,
        dataset_id: str,
        dax_query: str,
    ) -> dict[str, Any]:
        """Execute a DAX query against a dataset and measure round-trip time.

        Calls the *Execute Queries* REST API and times the request. Used for
        active response-time probing (a no-Log-Analytics alternative).

        Parameters
        ----------
        workspace_id : str
            Power BI workspace (group) ID.
        dataset_id : str
            Power BI dataset (semantic model) ID.
        dax_query : str
            A DAX query (e.g. ``EVALUATE ROW("x", 1)``).

        Returns
        -------
        dict
            ``{"duration_ms": float, "row_count": int}`` on success.

        Raises
        ------
        requests.HTTPError
            On non-retryable HTTP errors.

        Notes
        -----
        The shared retry strategy only auto-retries GET, so this POST is
        issued once and timed cleanly (auto-retries would inflate timing).
        """
        url = f"{BASE_URL}/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        body = {
            "queries": [{"query": dax_query}],
            "serializerSettings": {"includeNulls": True},
        }

        start = time.perf_counter()
        response = self._session.post(
            url,
            headers=self._headers,
            json=body,
            timeout=self._request_timeout,
        )
        duration_ms = (time.perf_counter() - start) * 1000.0
        response.raise_for_status()

        payload = response.json()
        return {"duration_ms": duration_ms, "row_count": _count_rows(payload)}

    def get_activity_events(
        self,
        start_datetime: str,
        end_datetime: str,
    ) -> list[dict[str, Any]]:
        """Fetch Power BI activity (audit) events for a single UTC day.

        Calls the admin ``activityevents`` API and follows continuation tokens
        until the full result set for the window has been retrieved.

        Parameters
        ----------
        start_datetime, end_datetime : str
            ISO-8601 UTC timestamps **without** offset (e.g.
            ``2024-06-01T00:00:00`` / ``2024-06-01T23:59:59``). The API requires
            both to fall within the same UTC day.

        Returns
        -------
        list[dict]
            All activity event entities in the window.

        Raises
        ------
        requests.HTTPError
            On non-retryable HTTP errors (401/403 when the read-only admin API
            tenant setting is not enabled for the service principal).
        """
        # The API requires literal single-quotes around the timestamps in the URL.
        # Using params= causes requests to percent-encode them (%27), which the
        # API rejects with HTTP 400, so we embed them directly in the URL string.
        url = (
            f"{BASE_URL}/admin/activityevents"
            f"?startDateTime='{start_datetime}'&endDateTime='{end_datetime}'"
        )

        response = self._session.get(
            url,
            headers=self._headers,
            timeout=self._request_timeout,
        )
        _raise_if_throttled(response, f"activityevents [{start_datetime}]")
        response.raise_for_status()
        payload = response.json()

        events: list[dict[str, Any]] = list(payload.get("activityEventEntities", []))
        continuation_token = payload.get("continuationToken")
        continuation_uri = payload.get("continuationUri")

        while continuation_token and continuation_uri:
            response = self._session.get(
                continuation_uri,
                headers=self._headers,
                timeout=self._request_timeout,
            )
            _raise_if_throttled(response, f"activityevents continuation [{start_datetime}]")
            response.raise_for_status()
            payload = response.json()
            events.extend(payload.get("activityEventEntities", []))
            continuation_token = payload.get("continuationToken")
            continuation_uri = payload.get("continuationUri")

        return events

    def get_activity_events_safe(
        self,
        start_datetime: str,
        end_datetime: str,
    ) -> list[dict[str, Any]]:
        """Fetch activity events with graceful error handling.

        Logs a clear message for the common 401/403 case — the
        "Allow service principals to use read-only admin APIs" tenant setting
        not yet enabled/scoped — and returns an empty list instead of raising.

        Throttling (HTTP 429 / :class:`RateLimitError`) is **deliberately not
        swallowed**: it is re-raised so the caller stops on the throttled day
        rather than silently returning ``[]``. Because the pipeline fetches days
        oldest-first, stopping keeps the watermark at the last fully-loaded day
        and the throttled day is retried on the next run — no silent gaps.
        Other unexpected errors are re-raised.
        """
        try:
            return self.get_activity_events(start_datetime, end_datetime)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in (401, 403):
                logger.error(
                    "Access denied (HTTP %s) calling activityevents for %s. Ensure the "
                    "'Allow service principals to use read-only admin APIs' tenant setting "
                    "is enabled and scoped to the service principal's security group.",
                    status,
                    start_datetime,
                )
                return []
            raise

    def close(self) -> None:
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False
