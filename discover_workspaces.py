"""
Lists all workspaces (groups) your Service Principal has access to.
Run this first to find a workspace_id, then use discover_datasets.py.

Required env vars:
  POWERBI_TENANT_ID
  POWERBI_CLIENT_ID
  POWERBI_CLIENT_SECRET

PowerShell:
  $env:POWERBI_TENANT_ID="..."; $env:POWERBI_CLIENT_ID="..."; $env:POWERBI_CLIENT_SECRET="..."
  python discover_workspaces.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from src.auth import PowerBIAuthenticator

TENANT_ID     = os.environ.get("POWERBI_TENANT_ID")
CLIENT_ID     = os.environ.get("POWERBI_CLIENT_ID")
CLIENT_SECRET = os.environ.get("POWERBI_CLIENT_SECRET")

missing = [k for k, v in {
    "POWERBI_TENANT_ID": TENANT_ID,
    "POWERBI_CLIENT_ID": CLIENT_ID,
    "POWERBI_CLIENT_SECRET": CLIENT_SECRET,
}.items() if not v]

if missing:
    print(f"ERROR: Missing environment variables: {', '.join(missing)}")
    sys.exit(1)

import requests

print("\nAuthenticating...")
auth = PowerBIAuthenticator(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
token = auth.get_access_token()
print("Token acquired. Fetching workspaces...\n")

response = requests.get(
    "https://api.powerbi.com/v1.0/myorg/groups",
    headers={"Authorization": f"Bearer {token}"},
    params={"$top": 100},
    timeout=30,
)
response.raise_for_status()
workspaces = response.json().get("value", [])

if not workspaces:
    print("No workspaces found.")
    print("Make sure your Service Principal has been added as a Member to at least one workspace.")
    sys.exit(0)

print(f"{'WORKSPACE NAME':<45} {'WORKSPACE ID'}")
print("-" * 85)
for ws in workspaces:
    print(f"{ws.get('name', '(unknown)'):<45} {ws.get('id', '(unknown)')}")

print(f"\nTotal: {len(workspaces)} workspace(s)")
print("\nCopy a workspace ID above and set:")
print("  $env:POWERBI_WORKSPACE_ID=\"<paste id here>\"")
print("Then run:  python discover_datasets.py")
