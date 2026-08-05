"""
Extracts Power BI data via the REST API and saves it as CSV files locally.
Use this as a lightweight alternative to Delta Lake while Databricks access
is being provisioned.

Output files (created in ./output/):
  refresh_history.csv      — latest refresh records per dataset
  activity_events.csv      — daily activity events (deduplicated on event_id)

Credentials (sensitive — keep as env vars):
  POWERBI_TENANT_ID
  POWERBI_CLIENT_ID
  POWERBI_CLIENT_SECRET

All other settings are configured in the CONFIGURATION block below.

PowerShell:
  $env:POWERBI_TENANT_ID="..."; $env:POWERBI_CLIENT_ID="..."; $env:POWERBI_CLIENT_SECRET="..."
  python extract_to_csv.py
"""

import argparse
import csv
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.ERROR, format="%(name)s %(levelname)s: %(message)s")

sys.path.insert(0, os.path.dirname(__file__))

from src.auth import PowerBIAuthenticator
from src.api_client import PowerBIClient
from src.transform import transform_refresh_records, transform_activity_events
from src.usage import day_bounds

# ===========================================================================
# CONFIGURATION — edit these values before running
# ===========================================================================

# --- Refresh History -------------------------------------------------------
# Workspace and dataset to pull refresh history for.
# Leave WORKSPACE_ID as None to skip refresh history entirely.
# Leave DATASET_ID as None to auto-discover all datasets in the workspace.
WORKSPACE_ID = None          # e.g. "f089354e-8366-4e18-aea3-4cb4a3a50b48"
DATASET_ID   = None          # e.g. "cfafbeb1-8037-4d0c-896e-a46fb27ff229"  (or None = all)
DATASET_NAME = "unknown"     # friendly name shown in logs

# --- Activity Events -------------------------------------------------------
# Option A: fixed date range — set both START_DATE and END_DATE.
# Option B: rolling lookback — set START_DATE = None and adjust LOOKBACK_DAYS.
#
# NOTE: The API only retains ~28 days. Dates older than that return 0 events.
ACTIVITY_START_DATE = None   # e.g. "2026-06-10"  (YYYY-MM-DD), or None
ACTIVITY_END_DATE   = None   # e.g. "2026-06-30"  (YYYY-MM-DD), or None = yesterday
LOOKBACK_DAYS       = 7      # used only when ACTIVITY_START_DATE is None

# ===========================================================================

# ---------------------------------------------------------------------------
# Argument parsing — CLI args override the config block above when provided
# ---------------------------------------------------------------------------
_parser = argparse.ArgumentParser(
    description="Extract Power BI data to local CSV files.",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
examples:
  # rolling last 7 days (uses config block defaults)
  python extract_to_csv.py

  # specific date range
  python extract_to_csv.py --start-date 2026-06-10 --end-date 2026-06-30

  # specific workspace, last 14 days
  python extract_to_csv.py --workspace-id f089354e-8366-4e18-aea3-4cb4a3a50b48 --lookback-days 14
""",
)
_parser.add_argument("--workspace-id",  default=None, help="Power BI workspace (group) UUID")
_parser.add_argument("--dataset-id",   default=None, help="Dataset UUID (omit to discover all in workspace)")
_parser.add_argument("--dataset-name", default=None, help="Friendly dataset name shown in logs")
_parser.add_argument("--start-date",   default=None, metavar="YYYY-MM-DD", help="Activity events start date")
_parser.add_argument("--end-date",     default=None, metavar="YYYY-MM-DD", help="Activity events end date (default: yesterday)")
_parser.add_argument("--lookback-days",default=None, type=int, metavar="N", help="Rolling lookback days when --start-date is not set")
_args = _parser.parse_args()

# Merge: CLI arg wins over config block value when explicitly provided
WORKSPACE_ID        = _args.workspace_id   or WORKSPACE_ID
DATASET_ID          = _args.dataset_id     or DATASET_ID
DATASET_NAME        = _args.dataset_name   or DATASET_NAME
ACTIVITY_START_DATE = _args.start_date     or ACTIVITY_START_DATE
ACTIVITY_END_DATE   = _args.end_date       or ACTIVITY_END_DATE
LOOKBACK_DAYS       = _args.lookback_days  or LOOKBACK_DAYS

# Credentials stay as env vars (never hard-code secrets)
TENANT_ID     = os.environ.get("POWERBI_TENANT_ID")
CLIENT_ID     = os.environ.get("POWERBI_CLIENT_ID")
CLIENT_SECRET = os.environ.get("POWERBI_CLIENT_SECRET")

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def write_csv(filename: str, rows: list[dict], mode: str = "w") -> int:
    """Write rows to a CSV file. Returns the number of rows written."""
    if not rows:
        return 0
    path = os.path.join(OUTPUT_DIR, filename)
    file_exists = os.path.isfile(path)
    fieldnames = list(rows[0].keys())
    with open(path, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if mode == "w" or not file_exists:
            writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def read_existing_ids(filename: str, id_field: str) -> set:
    """Read IDs already present in a CSV to avoid duplicates on re-run."""
    path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.isfile(path):
        return set()
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row[id_field] for row in reader if id_field in row}


def read_all_activity_ids(id_field: str) -> set:
    """Scan all activity_events_*.csv files in OUTPUT_DIR for existing IDs."""
    existing: set = set()
    for fname in os.listdir(OUTPUT_DIR):
        if fname.startswith("activity_events_") and fname.endswith(".csv"):
            existing |= read_existing_ids(fname, id_field)
    return existing


def activity_filename(year: int, month: int) -> str:
    """Return the monthly CSV filename, e.g. activity_events_2026_06.csv."""
    return f"activity_events_{year}_{month:02d}.csv"


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


# ---------------------------------------------------------------------------
# Validate credentials
# ---------------------------------------------------------------------------
missing = [k for k, v in {
    "POWERBI_TENANT_ID": TENANT_ID,
    "POWERBI_CLIENT_ID": CLIENT_ID,
    "POWERBI_CLIENT_SECRET": CLIENT_SECRET,
}.items() if not v]

if missing:
    print(f"ERROR: Missing environment variables: {', '.join(missing)}")
    sys.exit(1)

ensure_output_dir()

log("Authenticating with Power BI API...")
auth = PowerBIAuthenticator(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
token = auth.get_access_token()
log("Token acquired.")

# ---------------------------------------------------------------------------
# Refresh History
# ---------------------------------------------------------------------------
if WORKSPACE_ID:
    log(f"Fetching refresh history — workspace={WORKSPACE_ID}")

    with PowerBIClient(access_token=token) as client:
        if DATASET_ID:
            dataset_list = [{"id": DATASET_ID, "name": DATASET_NAME}]
        else:
            log("No POWERBI_DATASET_ID set — discovering all datasets in workspace...")
            raw_datasets = client.get_datasets_in_workspace(WORKSPACE_ID)
            dataset_list = [{"id": ds["id"], "name": ds.get("name", ds["id"])} for ds in raw_datasets]
            log(f"  Found {len(dataset_list)} dataset(s).")

        all_refresh_rows = []
        for ds in dataset_list:
            raw = client.get_refresh_history_safe(
                workspace_id=WORKSPACE_ID,
                dataset_id=ds["id"],
                dataset_name=ds["name"],
                top=60,
            )
            if raw:
                rows = transform_refresh_records(
                    raw_records=raw,
                    dataset_id=ds["id"],
                    dataset_name=ds["name"],
                    workspace_id=WORKSPACE_ID,
                    workspace_name="",
                    is_critical=False,
                )
                all_refresh_rows.extend(rows)
                log(f"  {ds['name']}: {len(rows)} refresh record(s)")
            else:
                log(f"  {ds['name']}: no records returned")

    if all_refresh_rows:
        written = write_csv("refresh_history.csv", all_refresh_rows, mode="w")
        log(f"Saved {written} refresh record(s) → output/refresh_history.csv")
    else:
        log("No refresh history to save.")
else:
    log("POWERBI_WORKSPACE_ID not set — skipping refresh history.")

# ---------------------------------------------------------------------------
# Activity Events
# ---------------------------------------------------------------------------
today = datetime.now(timezone.utc).date()
api_retention_cutoff = today - timedelta(days=28)

if ACTIVITY_START_DATE:
    from datetime import date as _date
    fetch_start = _date.fromisoformat(ACTIVITY_START_DATE)
    fetch_end   = _date.fromisoformat(ACTIVITY_END_DATE) if ACTIVITY_END_DATE else today - timedelta(days=1)
    if fetch_start < api_retention_cutoff:
        log(f"WARNING: ACTIVITY_START_DATE {fetch_start} is older than the API's ~28-day retention window.")
        log(f"         Events before {api_retention_cutoff} are gone from the API and will return 0 results.")
    date_range = [fetch_start + timedelta(days=i)
                  for i in range((fetch_end - fetch_start).days + 1)]
    log(f"Fetching activity events from {fetch_start} to {fetch_end} ({len(date_range)} day(s))...")
else:
    date_range = [today - timedelta(days=offset) for offset in range(LOOKBACK_DAYS, 0, -1)]
    log(f"Fetching activity events for the last {LOOKBACK_DAYS} day(s)...")

existing_ids = read_all_activity_ids("event_id")
log(f"  {len(existing_ids)} existing event(s) across all monthly files (will skip duplicates).")

# Collect new rows grouped by (year, month) for monthly file splitting
monthly_buckets: dict = defaultdict(list)

with PowerBIClient(access_token=token) as client:
    for target_date in date_range:
        start_dt, end_dt = day_bounds(target_date)
        raw_events = client.get_activity_events_safe(start_dt, end_dt)

        if not raw_events:
            log(f"  {target_date}: 0 events")
            continue

        rows = transform_activity_events(raw_events)
        new_rows = [r for r in rows if r.get("event_id") not in existing_ids]
        existing_ids.update(r["event_id"] for r in new_rows if r.get("event_id"))
        log(f"  {target_date}: {len(raw_events)} raw events → {len(new_rows)} new after dedup")

        for row in new_rows:
            monthly_buckets[(target_date.year, target_date.month)].append(row)

if monthly_buckets:
    for (year, month), rows in sorted(monthly_buckets.items()):
        fname = activity_filename(year, month)
        written = write_csv(fname, rows, mode="a" if os.path.isfile(os.path.join(OUTPUT_DIR, fname)) else "w")
        log(f"Saved {written} new event(s) → output/{fname}")
else:
    log("No new activity events to save.")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("  EXTRACTION COMPLETE")
print("=" * 60)
summary_files = ["refresh_history.csv"] + sorted(
    f for f in os.listdir(OUTPUT_DIR) if f.startswith("activity_events_") and f.endswith(".csv")
)
for fname in summary_files:
    path = os.path.join(OUTPUT_DIR, fname)
    if os.path.isfile(path):
        with open(path, newline="", encoding="utf-8") as f:
            row_count = sum(1 for _ in f) - 1
        size_kb = os.path.getsize(path) / 1024
        print(f"  {fname:<40} {row_count:>6} rows   {size_kb:.1f} KB")
print(f"\n  Files saved to: {OUTPUT_DIR}")
