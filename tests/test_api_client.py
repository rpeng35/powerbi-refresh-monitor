"""Unit tests for the api_client module."""

import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.api_client import PowerBIClient


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
