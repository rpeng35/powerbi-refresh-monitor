"""Unit tests for the auth module."""

from unittest.mock import MagicMock, patch

import pytest

from src.auth import PowerBIAuthenticator


class TestPowerBIAuthenticator:
    TENANT = "test-tenant-id"
    CLIENT = "test-client-id"
    SECRET = "test-secret"

    def _make_authenticator(self):
        with patch("src.auth.msal.ConfidentialClientApplication"):
            return PowerBIAuthenticator(self.TENANT, self.CLIENT, self.SECRET)

    def test_returns_cached_token(self):
        auth = self._make_authenticator()
        auth._app.acquire_token_silent.return_value = {"access_token": "cached-token"}

        token = auth.get_access_token()

        assert token == "cached-token"
        auth._app.acquire_token_silent.assert_called_once()
        auth._app.acquire_token_for_client.assert_not_called()

    def test_acquires_new_token_when_cache_empty(self):
        auth = self._make_authenticator()
        auth._app.acquire_token_silent.return_value = None
        auth._app.acquire_token_for_client.return_value = {"access_token": "new-token"}

        token = auth.get_access_token()

        assert token == "new-token"
        auth._app.acquire_token_for_client.assert_called_once()

    def test_raises_on_auth_failure(self):
        auth = self._make_authenticator()
        auth._app.acquire_token_silent.return_value = None
        auth._app.acquire_token_for_client.return_value = {
            "error": "invalid_client",
            "error_description": "Client secret expired",
        }

        with pytest.raises(RuntimeError, match="Client secret expired"):
            auth.get_access_token()

    def test_raises_with_generic_message_on_unknown_error(self):
        auth = self._make_authenticator()
        auth._app.acquire_token_silent.return_value = None
        auth._app.acquire_token_for_client.return_value = {}

        with pytest.raises(RuntimeError, match="Unknown authentication error"):
            auth.get_access_token()
