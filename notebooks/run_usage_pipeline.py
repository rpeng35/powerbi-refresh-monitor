# Databricks notebook source

# MAGIC %md
# MAGIC # Power BI Usage (Activity Events) — Daily Ingestion Pipeline
# MAGIC
# MAGIC Extracts Power BI **activity (audit) events** via the admin `activityevents`
# MAGIC API, normalizes them, and merges into a Delta table for usage / popularity
# MAGIC analysis — most-used reports, least-used / unused reports, and per-user views.
# MAGIC
# MAGIC **Why run this daily:** the Activity Events API retains only **~28 days** of
# MAGIC history. Power BI does not keep it forever — every daily run banks another day
# MAGIC into Delta, building the long-term history the API itself won't keep.
# MAGIC
# MAGIC **Schedule:** Daily via Databricks Workflow.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Service Principal with the **"Allow service principals to use read-only admin APIs"**
# MAGIC   tenant setting enabled and scoped to its security group (admin API — a permission
# MAGIC   toggle, no extra cost).
# MAGIC - Credentials in Databricks Secret Scope `powerbi-monitor`.
# MAGIC - `config/pipeline_config.json` populated (`activity_events_table`, `usage`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Configuration

# COMMAND ----------

import json
import logging
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("powerbi_usage_pipeline")

# Add the repo root to the Python path so local modules are importable.
REPO_ROOT = "/Workspace/Repos/<user>/powerbi-refresh-monitor"
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.auth import PowerBIAuthenticator
from src.api_client import PowerBIClient
from src.transform import transform_activity_events
from src.usage import get_dates_to_fetch, day_bounds
from src.delta_ops import (
    ensure_activity_events_table_exists,
    get_max_activity_date,
    upsert_activity_events,
    ACTIVITY_EVENTS_SCHEMA,
)

# COMMAND ----------

with open(f"{REPO_ROOT}/config/pipeline_config.json") as f:
    pipeline_config = json.load(f)

ae_cfg = pipeline_config["activity_events_table"]
TABLE_NAME = f"{ae_cfg['catalog']}.{ae_cfg['schema']}.{ae_cfg['table_name']}"

api_cfg = pipeline_config["api"]
usage_cfg = pipeline_config.get("usage", {})
lookback_days = usage_cfg.get("lookback_days", 28)
tracked_activities = usage_cfg.get("tracked_activities")  # None = all activities
incremental = usage_cfg.get("incremental", True)

logger.info("Configuration loaded. Target table: %s", TABLE_NAME)
logger.info("Lookback days: %d | tracked activities: %s", lookback_days, tracked_activities or "ALL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Authenticate with Power BI API

# COMMAND ----------

secrets_cfg = pipeline_config["secrets"]
tenant_id = dbutils.secrets.get(scope=secrets_cfg["scope"], key=secrets_cfg["tenant_id_key"])
client_id = dbutils.secrets.get(scope=secrets_cfg["scope"], key=secrets_cfg["client_id_key"])
client_secret = dbutils.secrets.get(scope=secrets_cfg["scope"], key=secrets_cfg["client_secret_key"])

authenticator = PowerBIAuthenticator(tenant_id, client_id, client_secret)
access_token = authenticator.get_access_token()
logger.info("Successfully acquired Power BI access token.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Determine the Date Window (incremental, day-by-day)

# COMMAND ----------

# Ensure the table exists first so the watermark lookup is clean.
ensure_activity_events_table_exists(spark, TABLE_NAME)

last_loaded_date = get_max_activity_date(spark, TABLE_NAME) if incremental else None
dates_to_fetch = get_dates_to_fetch(lookback_days, last_loaded_date=last_loaded_date)

logger.info(
    "Last loaded activity date: %s. Days to fetch: %d (%s ... %s).",
    last_loaded_date,
    len(dates_to_fetch),
    dates_to_fetch[0] if dates_to_fetch else "-",
    dates_to_fetch[-1] if dates_to_fetch else "-",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Extract & Transform Activity Events

# COMMAND ----------

all_rows = []

with PowerBIClient(
    access_token=access_token,
    max_retries=api_cfg["max_retries"],
    backoff_factor=api_cfg["backoff_factor"],
    request_timeout=api_cfg["request_timeout_seconds"],
) as client:
    for day in dates_to_fetch:
        start_dt, end_dt = day_bounds(day)
        logger.info("Fetching activity events for %s ...", day)

        raw_events = client.get_activity_events_safe(start_dt, end_dt)
        rows = transform_activity_events(raw_events, tracked_activities=tracked_activities)
        all_rows.extend(rows)
        logger.info("  -> %d raw events, %d rows kept.", len(raw_events), len(rows))

        # Respect API rate limits between days.
        time.sleep(api_cfg.get("inter_request_delay_seconds", 0.2))

logger.info("Total activity rows extracted: %d", len(all_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Merge into Delta Table

# COMMAND ----------

if all_rows:
    new_data_df = spark.createDataFrame(all_rows, schema=ACTIVITY_EVENTS_SCHEMA)
    upsert_activity_events(spark, new_data_df, TABLE_NAME)
    logger.info("Delta table '%s' updated successfully.", TABLE_NAME)
else:
    logger.warning("No activity events to load for the selected window.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Quick Usage Summary (most / least used reports)

# COMMAND ----------

print("=" * 60)
print("  USAGE PIPELINE RUN SUMMARY")
print("=" * 60)
print(f"  Days fetched:     {len(dates_to_fetch)}")
print(f"  Rows ingested:    {len(all_rows)}")
print(f"  Target table:     {TABLE_NAME}")
print("=" * 60)

if all_rows:
    print("\nTop 10 most-viewed reports (last 30 days):")
    spark.sql(
        f"""
        SELECT report_name, workspace_name,
               COUNT(*) AS views,
               COUNT(DISTINCT user_id) AS distinct_users
        FROM {TABLE_NAME}
        WHERE activity = 'ViewReport'
          AND creation_date >= date_sub(current_date(), 30)
          AND report_name IS NOT NULL
        GROUP BY report_name, workspace_name
        ORDER BY views DESC
        LIMIT 10
        """
    ).show(truncate=False)
