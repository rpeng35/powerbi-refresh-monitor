"""
OAuth2 token acquisition for Power BI REST API via Microsoft Entra ID Service Principal.

Uses MSAL ConfidentialClientApplication with client credentials flow.
Credentials are read from Databricks Secrets at runtime.
"""

import logging

import msal

logger = logging.getLogger(__name__)

POWER_BI_SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]


class PowerBIAuthenticator:
    """Acquires and caches OAuth2 tokens for Power BI REST API access."""

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        self._authority = f"https://login.microsoftonline.com/{tenant_id}"
        self._app = msal.ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=self._authority,
        )

    def get_access_token(self) -> str:
        """Return a valid access token, using the MSAL cache when possible.

        Raises
        ------
        RuntimeError
            If token acquisition fails (e.g. expired secret, misconfigured app).
        """
        # Try the in-memory cache first to avoid unnecessary token requests
        result = self._app.acquire_token_silent(scopes=POWER_BI_SCOPE, account=None)
        if not result:
            logger.info("No cached token found; acquiring new token via client credentials.")
            result = self._app.acquire_token_for_client(scopes=POWER_BI_SCOPE)

        if "access_token" in result:
            return result["access_token"]

        error = result.get("error_description", result.get("error", "Unknown authentication error"))
        raise RuntimeError(f"Failed to acquire Power BI access token: {error}")


def create_authenticator_from_secrets(dbutils) -> PowerBIAuthenticator:
    """Factory that reads credentials from a Databricks Secret Scope.

    Parameters
    ----------
    dbutils : object
        Databricks ``dbutils`` instance (available in notebook context).

    Returns
    -------
    PowerBIAuthenticator
    """
    scope = "powerbi-monitor"
    tenant_id = dbutils.secrets.get(scope=scope, key="tenant-id")
    client_id = dbutils.secrets.get(scope=scope, key="client-id")
    client_secret = dbutils.secrets.get(scope=scope, key="client-secret")
    return PowerBIAuthenticator(tenant_id, client_id, client_secret)
