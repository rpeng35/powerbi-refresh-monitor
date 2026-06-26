"""
Demo script — calls the live Power BI REST API and shows what data is extracted
and how it is structured after transformation.

Run from the project root:  python demo_output.py

Required env vars (all sections):
  POWERBI_TENANT_ID      Azure AD tenant ID
  POWERBI_CLIENT_ID      Service Principal application (client) ID
  POWERBI_CLIENT_SECRET  Service Principal client secret

Required env vars (refresh history sections 1-3 only):
  POWERBI_WORKSPACE_ID   Power BI workspace (group) ID
  POWERBI_DATASET_ID     Power BI dataset (semantic model) ID
  POWERBI_DATASET_NAME   Human-readable name for display (optional)

PowerShell example:
  $env:POWERBI_TENANT_ID="..."; $env:POWERBI_CLIENT_ID="..."; $env:POWERBI_CLIENT_SECRET="..."
  $env:POWERBI_WORKSPACE_ID="..."; $env:POWERBI_DATASET_ID="..."
  python demo_output.py
"""

import json
import sys
import os
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))

from src.auth import PowerBIAuthenticator
from src.api_client import PowerBIClient
from src.transform import (
    transform_refresh_records,
    transform_activity_events,
    filter_records_by_watermark,
)
from src.usage import day_bounds

DIVIDER = "=" * 70


def section(title):
    print(f"\n{DIVIDER}")
    print(f"  {title}")
    print(DIVIDER)


def print_row(row: dict, indent: int = 2):
    pad = " " * indent
    for k, v in row.items():
        print(f"{pad}{k:<32} {v}")


def missing(var):
    print(f"\n  [SKIPPED] Set the {var} environment variable to enable this section.")


# ---------------------------------------------------------------------------
# Read credentials
# ---------------------------------------------------------------------------
TENANT_ID     = os.environ.get("POWERBI_TENANT_ID")
CLIENT_ID     = os.environ.get("POWERBI_CLIENT_ID")
CLIENT_SECRET = os.environ.get("POWERBI_CLIENT_SECRET")
WORKSPACE_ID  = os.environ.get("POWERBI_WORKSPACE_ID")
DATASET_ID    = os.environ.get("POWERBI_DATASET_ID")
DATASET_NAME  = os.environ.get("POWERBI_DATASET_NAME", DATASET_ID)

has_creds    = all([TENANT_ID, CLIENT_ID, CLIENT_SECRET])
has_dataset  = all([WORKSPACE_ID, DATASET_ID])

if not has_creds:
    print("\n  ERROR: POWERBI_TENANT_ID, POWERBI_CLIENT_ID, and POWERBI_CLIENT_SECRET must all be set.")
    sys.exit(1)

print("\n  Authenticating with Power BI API...")
authenticator = PowerBIAuthenticator(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
token = authenticator.get_access_token()
print("  Token acquired successfully.")

# ---------------------------------------------------------------------------
# 1. Raw Refresh History
# ---------------------------------------------------------------------------
section("1. RAW Refresh History  (what the API returns — Get Refresh History In Group)")

if not has_dataset:
    missing("POWERBI_WORKSPACE_ID and POWERBI_DATASET_ID")
    raw_refreshes = []
else:
    with PowerBIClient(access_token=token) as client:
        raw_refreshes = client.get_refresh_history_safe(
            workspace_id=WORKSPACE_ID,
            dataset_id=DATASET_ID,
            dataset_name=DATASET_NAME,
            top=10,
        )
    print(f"\n  Dataset:  {DATASET_NAME}")
    print(f"  Records returned by API: {len(raw_refreshes)}")
    if raw_refreshes:
        print(f"\n  First raw record (full API payload):")
        print(json.dumps(raw_refreshes[0], indent=2))
        if len(raw_refreshes) > 1:
            print(f"  ... ({len(raw_refreshes) - 1} more records)")

# ---------------------------------------------------------------------------
section("2. TRANSFORMED Refresh Records  (what lands in Delta Lake)")

if not raw_refreshes:
    missing("POWERBI_WORKSPACE_ID and POWERBI_DATASET_ID")
else:
    rows = transform_refresh_records(
        raw_records=raw_refreshes,
        dataset_id=DATASET_ID,
        dataset_name=DATASET_NAME,
        workspace_id=WORKSPACE_ID,
        workspace_name="(from API)",
        is_critical=True,
    )
    for i, row in enumerate(rows[:5]):
        print(f"\n  --- Record {i + 1} (status={row['status']}) ---")
        print_row(row)
    if len(rows) > 5:
        print(f"\n  ... ({len(rows) - 5} more records)")

# ---------------------------------------------------------------------------
section("3. INCREMENTAL FILTERING  (watermark skips already-loaded records)")

if not raw_refreshes:
    missing("POWERBI_WORKSPACE_ID and POWERBI_DATASET_ID")
else:
    sorted_times = sorted(
        [r["startTime"] for r in raw_refreshes if r.get("startTime")]
    )
    if len(sorted_times) >= 2:
        watermark = sorted_times[len(sorted_times) // 2]
    elif sorted_times:
        watermark = sorted_times[0]
    else:
        watermark = None

    print(f"\n  Simulated watermark (mid-point of fetched records): {watermark}")
    print(f"  Records before filtering: {len(raw_refreshes)}")
    filtered = filter_records_by_watermark(raw_refreshes, watermark)
    print(f"  Records after filtering:  {len(filtered)}  (only these would be processed on a re-run)")
    for r in filtered:
        print(f"    -> requestId={r.get('requestId')}  startTime={r.get('startTime')}  status={r.get('status')}")

# ---------------------------------------------------------------------------
section("4. RAW Activity Events  (what the admin activityevents API returns)")

yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
start_dt, end_dt = day_bounds(yesterday)
print(f"\n  Fetching events for: {yesterday}  ({start_dt}  →  {end_dt})")

with PowerBIClient(access_token=token) as client:
    raw_events = client.get_activity_events(start_dt, end_dt)

print(f"\n  Total events returned by API: {len(raw_events)}")
if raw_events:
    print(f"\n  First raw event (full API payload):")
    print(json.dumps(raw_events[0], indent=2))
    if len(raw_events) > 1:
        print(f"  ... ({len(raw_events) - 1} more events)")
else:
    print("  No events returned — the tenant may have no activity yesterday or the SP lacks admin API access.")

# ---------------------------------------------------------------------------
section("5. TRANSFORMED Activity Events  (what lands in Delta Lake)")

if raw_events:
    event_rows = transform_activity_events(raw_events)
    print(f"\n  Total rows after transformation: {len(event_rows)}")
    for i, row in enumerate(event_rows[:3]):
        print(f"\n  --- Event {i + 1} (activity={row['activity']}) ---")
        print_row(row)
    if len(event_rows) > 3:
        print(f"\n  ... ({len(event_rows) - 3} more rows)")
else:
    print("\n  No events to transform.")

# ---------------------------------------------------------------------------
section("6. FILTERED Activity Events  (ViewReport only — usage/popularity)")

if raw_events:
    view_rows = transform_activity_events(raw_events, tracked_activities=["ViewReport"])
    print(f"\n  'ViewReport' events: {len(view_rows)} of {len(raw_events)} total")
    for row in view_rows[:10]:
        print(f"    user={row['user_id']}  report={row['report_name']}  date={row['creation_date']}")
    if len(view_rows) > 10:
        print(f"  ... ({len(view_rows) - 10} more)")
else:
    print("\n  No events to filter.")

# ---------------------------------------------------------------------------
section("SUMMARY")
print(f"""
  Credentials:   tenant={TENANT_ID}  client={CLIENT_ID}
  Dataset:       {DATASET_NAME or '(not set)'}  workspace={WORKSPACE_ID or '(not set)'}
  Activity date: {yesterday}  ({len(raw_events) if raw_events else 0} events)

  Pipeline produces 3 Delta Lake tables:

  powerbi_refresh_history    — one row per refresh attempt per dataset
    key fields: request_id, dataset_name, status, start_time,
                duration_seconds, error_code, is_critical

  powerbi_query_performance  — one row per DAX probe run per dataset
    key fields: probe_id, dataset_name, query_label, status, duration_ms

  powerbi_activity_events    — one row per audit event (report views, etc.)
    key fields: event_id, activity, user_id, report_name,
                workspace_name, creation_date
""")
