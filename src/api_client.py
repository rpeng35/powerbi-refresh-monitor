"""
Power BI REST API client for fetching refresh histories.

Handles HTTP session management, retry logic with exponential backoff,
and per-dataset refresh history retrieval.
"""

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

    def close(self) -> None:
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False
