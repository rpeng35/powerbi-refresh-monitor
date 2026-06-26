"""
Lists all datasets visible to your Service Principal in a given workspace.
Use this to find the workspace_id, dataset_id, and dataset name for demo_output.py.

Set env vars then run:
  python discover_datasets.py

Required:
  POWERBI_TENANT_ID
  POWERBI_CLIENT_ID
  POWERBI_CLIENT_SECRET
  POWERBI_WORKSPACE_ID   (get this from the URL in app.powerbi.com)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from src.auth import PowerBIAuthenticator
from src.api_client import PowerBIClient

TENANT_ID    = os.environ.get("POWERBI_TENANT_ID")
CLIENT_ID    = os.environ.get("POWERBI_CLIENT_ID")
CLIENT_SECRET = os.environ.get("POWERBI_CLIENT_SECRET")
WORKSPACE_ID = os.environ.get("POWERBI_WORKSPACE_ID")

missing = [k for k, v in {
    "POWERBI_TENANT_ID": TENANT_ID,
    "POWERBI_CLIENT_ID": CLIENT_ID,
    "POWERBI_CLIENT_SECRET": CLIENT_SECRET,
    "POWERBI_WORKSPACE_ID": WORKSPACE_ID,
}.items() if not v]

if missing:
    print(f"ERROR: Missing environment variables: {', '.join(missing)}")
    sys.exit(1)

print(f"\nAuthenticating...")
auth = PowerBIAuthenticator(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
token = auth.get_access_token()
print(f"Token acquired. Fetching datasets in workspace {WORKSPACE_ID}...\n")

with PowerBIClient(access_token=token) as client:
    datasets = client.get_datasets_in_workspace(WORKSPACE_ID)

if not datasets:
    print("No datasets found. Check that your Service Principal has access to this workspace.")
    sys.exit(0)

print(f"{'DATASET NAME':<45} {'DATASET ID':<40} REFRESHABLE")
print("-" * 95)
for ds in datasets:
    name = ds.get("name", "(unknown)")
    did  = ds.get("id", "(unknown)")
    refreshable = ds.get("isRefreshable", False)
    print(f"{name:<45} {did:<40} {refreshable}")

print(f"\nTotal: {len(datasets)} dataset(s)")
print(f"\nWorkspace ID: {WORKSPACE_ID}")
print("\nCopy the dataset_id and name above into your env vars:")
print("  $env:POWERBI_DATASET_ID=\"<paste id here>\"")
print("  $env:POWERBI_DATASET_NAME=\"<paste name here>\"")
