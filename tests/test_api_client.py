"""Unit tests for the api_client module."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.api_client import PowerBIClient, _count_rows


class TestPowerBIClient:
    SAMPLE_RESPONSE = {
        "value": [
            {
                "requestId": "req-1",
                "refreshType": "Scheduled",
                "startTime": "2024-06-01T08:00:00.000Z",
                "endTime": "2024-06-01T08:05:00.000Z",
                "status": "Completed",
            }
        ]
    }

    @patch("src.api_client.requests.Session")
    def test_get_refresh_history_success(self, mock_session_cls):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = self.SAMPLE_RESPONSE
        mock_response.raise_for_status.return_value = None
        mock_session.get.return_value = mock_response
        mock_session_cls.return_value = mock_session

        with PowerBIClient(access_token="test-token") as client:
            # Override the internal session with our mock
            client._session = mock_session
            records = client.get_refresh_history("ws-1", "ds-1")

        assert len(records) == 1
        assert records[0]["requestId"] == "req-1"

    @patch("src.api_client.requests.Session")
    def test_get_refresh_history_empty_response(self, mock_session_cls):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": []}
        mock_response.raise_for_status.return_value = None
        mock_session.get.return_value = mock_response
        mock_session_cls.return_value = mock_session

        with PowerBIClient(access_token="test-token") as client:
            client._session = mock_session
            records = client.get_refresh_history("ws-1", "ds-1")

        assert records == []

    @patch("src.api_client.requests.Session")
    def test_safe_returns_empty_on_404(self, mock_session_cls):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 404
        http_error = requests.HTTPError(response=mock_response)
        mock_session.get.side_effect = http_error
        mock_session_cls.return_value = mock_session

        with PowerBIClient(access_token="test-token") as client:
            client._session = mock_session
            records = client.get_refresh_history_safe("ws-1", "ds-1", "Test Dataset")

        assert records == []

    @patch("src.api_client.requests.Session")
    def test_safe_returns_empty_on_403(self, mock_session_cls):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 403
        http_error = requests.HTTPError(response=mock_response)
        mock_session.get.side_effect = http_error
        mock_session_cls.return_value = mock_session

        with PowerBIClient(access_token="test-token") as client:
            client._session = mock_session
            records = client.get_refresh_history_safe("ws-1", "ds-1", "Test Dataset")

        assert records == []

    @patch("src.api_client.requests.Session")
    def test_safe_raises_on_unexpected_error(self, mock_session_cls):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        http_error = requests.HTTPError(response=mock_response)
        mock_session.get.side_effect = http_error
        mock_session_cls.return_value = mock_session

        with PowerBIClient(access_token="test-token") as client:
            client._session = mock_session
            with pytest.raises(requests.HTTPError):
                client.get_refresh_history_safe("ws-1", "ds-1", "Test Dataset")


class TestWorkspaceDiscovery:
    SAMPLE_DATASETS = {
        "value": [
            {"id": "ds-1", "name": "Dataset One", "isRefreshable": True},
            {"id": "ds-2", "name": "Dataset Two", "isRefreshable": True},
        ]
    }

    @patch("src.api_client.requests.Session")
    def test_get_datasets_success(self, mock_session_cls):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = self.SAMPLE_DATASETS
        mock_response.raise_for_status.return_value = None
        mock_session.get.return_value = mock_response
        mock_session_cls.return_value = mock_session

        with PowerBIClient(access_token="test-token") as client:
            client._session = mock_session
            datasets = client.get_datasets_in_workspace("ws-1")

        assert len(datasets) == 2
        assert datasets[0]["id"] == "ds-1"
        assert datasets[1]["name"] == "Dataset Two"

    @patch("src.api_client.requests.Session")
    def test_get_datasets_empty(self, mock_session_cls):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": []}
        mock_response.raise_for_status.return_value = None
        mock_session.get.return_value = mock_response
        mock_session_cls.return_value = mock_session

        with PowerBIClient(access_token="test-token") as client:
            client._session = mock_session
            datasets = client.get_datasets_in_workspace("ws-1")

        assert datasets == []

    @patch("src.api_client.requests.Session")
    def test_safe_returns_empty_on_404(self, mock_session_cls):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 404
        http_error = requests.HTTPError(response=mock_response)
        mock_session.get.side_effect = http_error
        mock_session_cls.return_value = mock_session

        with PowerBIClient(access_token="test-token") as client:
            client._session = mock_session
            datasets = client.get_datasets_in_workspace_safe("ws-1", "Test WS")

        assert datasets == []

    @patch("src.api_client.requests.Session")
    def test_safe_returns_empty_on_403(self, mock_session_cls):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 403
        http_error = requests.HTTPError(response=mock_response)
        mock_session.get.side_effect = http_error
        mock_session_cls.return_value = mock_session

        with PowerBIClient(access_token="test-token") as client:
            client._session = mock_session
            datasets = client.get_datasets_in_workspace_safe("ws-1", "Test WS")

        assert datasets == []

    @patch("src.api_client.requests.Session")
    def test_safe_raises_on_unexpected_error(self, mock_session_cls):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        http_error = requests.HTTPError(response=mock_response)
        mock_session.get.side_effect = http_error
        mock_session_cls.return_value = mock_session

        with PowerBIClient(access_token="test-token") as client:
            client._session = mock_session
            with pytest.raises(requests.HTTPError):
                client.get_datasets_in_workspace_safe("ws-1", "Test WS")


class TestCountRows:
    def test_counts_rows(self):
        payload = {"results": [{"tables": [{"rows": [{"a": 1}, {"a": 2}, {"a": 3}]}]}]}
        assert _count_rows(payload) == 3

    def test_empty_rows(self):
        payload = {"results": [{"tables": [{"rows": []}]}]}
        assert _count_rows(payload) == 0

    def test_malformed_returns_zero(self):
        assert _count_rows({}) == 0
        assert _count_rows({"results": []}) == 0
        assert _count_rows({"results": [{}]}) == 0


class TestExecuteDaxQuery:
    SAMPLE_PAYLOAD = {
        "results": [{"tables": [{"rows": [{"x": 1}, {"x": 2}]}]}]
    }

    @patch("src.api_client.requests.Session")
    def test_success_returns_duration_and_rows(self, mock_session_cls):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = self.SAMPLE_PAYLOAD
        mock_response.raise_for_status.return_value = None
        mock_session.post.return_value = mock_response
        mock_session_cls.return_value = mock_session

        with PowerBIClient(access_token="test-token") as client:
            client._session = mock_session
            result = client.execute_dax_query("ws-1", "ds-1", 'EVALUATE ROW("x", 1)')

        assert result["row_count"] == 2
        assert isinstance(result["duration_ms"], float)
        assert result["duration_ms"] >= 0
        # Verify the POST body carried the DAX query.
        _, kwargs = mock_session.post.call_args
        assert kwargs["json"]["queries"][0]["query"] == 'EVALUATE ROW("x", 1)'

    @patch("src.api_client.requests.Session")
    def test_raises_on_http_error(self, mock_session_cls):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError()
        mock_session.post.return_value = mock_response
        mock_session_cls.return_value = mock_session

        with PowerBIClient(access_token="test-token") as client:
            client._session = mock_session
            with pytest.raises(requests.HTTPError):
                client.execute_dax_query("ws-1", "ds-1", "EVALUATE ROW(\"x\", 1)")
