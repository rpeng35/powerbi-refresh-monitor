"""
Demo script — shows what data the pipeline extracts and how it is structured.
Run from the project root:  python demo_output.py
No API credentials or Databricks connection required.
"""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from src.transform import (
    transform_refresh_records,
    transform_activity_events,
    filter_records_by_watermark,
)

DIVIDER = "=" * 70


def section(title):
    print(f"\n{DIVIDER}")
    print(f"  {title}")
    print(DIVIDER)


def print_row(row: dict, indent: int = 2):
    pad = " " * indent
    for k, v in row.items():
        print(f"{pad}{k:<32} {v}")


# ---------------------------------------------------------------------------
# 1. Refresh History Records
# ---------------------------------------------------------------------------
section("1. RAW Power BI API RESPONSE  (what the API returns)")

raw_refreshes = [
    {
        "requestId": "req-aaa-001",
        "refreshType": "Scheduled",
        "startTime": "2024-06-10T08:00:00.000Z",
        "endTime": "2024-06-10T08:07:32.000Z",
        "status": "Completed",
        "serviceExceptionJson": None,
        "refreshAttempts": [
            {"attemptId": 1, "type": "Data",
             "startTime": "2024-06-10T08:00:00.000Z",
             "endTime": "2024-06-10T08:07:32.000Z"}
        ],
    },
    {
        "requestId": "req-bbb-002",
        "refreshType": "Scheduled",
        "startTime": "2024-06-09T08:00:00.000Z",
        "endTime": "2024-06-09T08:02:10.000Z",
        "status": "Failed",
        "serviceExceptionJson": '{"errorCode":"ModelRefreshFailed_CredentialsNotSpecified","pbi.error":{"code":"ModelRefreshFailed"}}',
        "refreshAttempts": [],
    },
    {
        "requestId": "req-ccc-003",
        "refreshType": "ViaApi",
        "startTime": "2024-06-08T08:00:00.000Z",
        "endTime": None,
        "status": "Unknown",
        "serviceExceptionJson": None,
        "refreshAttempts": [],
    },
]

print(json.dumps(raw_refreshes[0], indent=2))
print("  ... (2 more records)")

# ---------------------------------------------------------------------------
section("2. TRANSFORMED Refresh Records  (what lands in Delta Lake)")

rows = transform_refresh_records(
    raw_records=raw_refreshes,
    dataset_id="cfafbeb1-8037-4d0c-896e-a46fb27ff229",
    dataset_name="Monthly Revenue Model",
    workspace_id="f089354e-8366-4e18-aea3-4cb4a3a50b48",
    workspace_name="Finance Reports",
    is_critical=True,
)

for i, row in enumerate(rows):
    print(f"\n  --- Record {i + 1} (status={row['status']}) ---")
    print_row(row)

# ---------------------------------------------------------------------------
section("3. INCREMENTAL FILTERING  (watermark skips already-loaded records)")

watermark = "2024-06-09T00:00:00Z"
print(f"\n  Watermark (last known start_time): {watermark}")
print(f"  Records before filtering: {len(raw_refreshes)}")

from src.transform import filter_records_by_watermark
filtered = filter_records_by_watermark(raw_refreshes, watermark)
print(f"  Records after filtering:  {len(filtered)}")
for r in filtered:
    print(f"    -> requestId={r['requestId']}  startTime={r['startTime']}  status={r['status']}")

# ---------------------------------------------------------------------------
section("4. RAW Activity Events  (what the admin activityevents API returns)")

raw_events = [
    {
        "Id": "evt-001",
        "CreationTime": "2024-06-10T13:45:00Z",
        "Activity": "ViewReport",
        "UserId": "alice@contoso.com",
        "WorkspaceId": "f089354e-8366-4e18-aea3-4cb4a3a50b48",
        "WorkspaceName": "Finance Reports",
        "ReportId": "rpt-xyz-111",
        "ReportName": "Monthly Revenue Dashboard",
        "DatasetId": "cfafbeb1-8037-4d0c-896e-a46fb27ff229",
        "DatasetName": "Monthly Revenue Model",
        "ResultStatus": "Succeeded",
    },
    {
        "Id": "evt-002",
        "CreationTime": "2024-06-10T14:10:00Z",
        "Activity": "ViewReport",
        "UserId": "bob@contoso.com",
        "WorkspaceId": "f089354e-8366-4e18-aea3-4cb4a3a50b48",
        "WorkspaceName": "Finance Reports",
        "ReportId": "rpt-xyz-111",
        "ReportName": "Monthly Revenue Dashboard",
        "DatasetId": "cfafbeb1-8037-4d0c-896e-a46fb27ff229",
        "DatasetName": "Monthly Revenue Model",
        "ResultStatus": "Succeeded",
    },
    {
        "Id": "evt-003",
        "CreationTime": "2024-06-10T15:00:00Z",
        "Activity": "CreateReport",
        "UserId": "charlie@contoso.com",
        "WorkspaceId": "f089354e-8366-4e18-aea3-4cb4a3a50b48",
        "WorkspaceName": "Finance Reports",
        "ReportName": "New Expense Report",
    },
]

print(json.dumps(raw_events[0], indent=2))
print("  ... (2 more events)")

# ---------------------------------------------------------------------------
section("5. TRANSFORMED Activity Events  (all event types)")

event_rows = transform_activity_events(raw_events)
for i, row in enumerate(event_rows):
    print(f"\n  --- Event {i + 1} (activity={row['activity']}) ---")
    print_row(row)

# ---------------------------------------------------------------------------
section("6. FILTERED Activity Events  (ViewReport only — usage/popularity)")

view_rows = transform_activity_events(raw_events, tracked_activities=["ViewReport"])
print(f"\n  Filtered to 'ViewReport': {len(view_rows)} of {len(raw_events)} events kept")
for row in view_rows:
    print(f"    user={row['user_id']}  report={row['report_name']}  date={row['creation_date']}")

# ---------------------------------------------------------------------------
section("SUMMARY")
print("""
  Pipeline produces 3 Delta Lake tables:

  powerbi_refresh_history    — one row per refresh attempt per dataset
    key fields: request_id, dataset_name, workspace_name, status,
                start_time, end_time, duration_seconds, error_code, is_critical

  powerbi_query_performance  — one row per DAX probe run per dataset
    key fields: probe_id, dataset_name, query_label,
                status, duration_ms, row_count, probe_timestamp

  powerbi_activity_events    — one row per audit event (report views, etc.)
    key fields: event_id, activity, user_id, report_name,
                workspace_name, creation_date

  All three tables are queryable in Databricks SQL immediately after ingestion.
""")
