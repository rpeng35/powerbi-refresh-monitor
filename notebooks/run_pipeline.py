# Databricks notebook source

# MAGIC %md
# MAGIC # Power BI Refresh History — Daily Ingestion Pipeline
# MAGIC
# MAGIC Extracts refresh metadata from the Power BI REST API for all configured datasets,
# MAGIC transforms the data, and upserts into a Delta Lake table. Optionally runs
# MAGIC post-ingestion alert checks.
# MAGIC
# MAGIC **Features:**
# MAGIC - **Workspace-level auto-discovery** — point at a workspace and all datasets are found.
# MAGIC - **Incremental extraction** — watermark per dataset, only new records are processed.
# MAGIC - **Concurrent API calls** — configurable thread pool for faster extraction at scale.
# MAGIC - **Partial-failure tolerance** — one failing dataset does not block the rest.
# MAGIC
# MAGIC **Schedule:** Daily at 02:00 AM PST via Databricks Workflow.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Service Principal registered in Entra ID with Power BI workspace access.
# MAGIC - Credentials stored in Databricks Secret Scope `powerbi-monitor`.
# MAGIC - `config/datasets.json` and `config/pipeline_config.json` populated.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Configuration

# COMMAND ----------

import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("powerbi_refresh_pipeline")

# Add the repo root to the Python path so local modules are importable.
# When deployed via Databricks Repos, the repo root is automatically on
# the path. For Workspace notebooks, adjust this path as needed.
REPO_ROOT = "/Workspace/Repos/<user>/powerbi-refresh-monitor"
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.auth import PowerBIAuthenticator
from src.api_client import PowerBIClient
from src.transform import transform_refresh_records, filter_records_by_watermark
from src.delta_ops import (
    ensure_table_exists,
    upsert_refresh_data,
    get_watermarks,
    REFRESH_HISTORY_SCHEMA,
)
from src.discovery import resolve_dataset_registry
from src.alerts import AlertDispatcher, check_refresh_failures, check_duration_anomalies, check_stale_data

# COMMAND ----------

# Load pipeline configuration
with open(f"{REPO_ROOT}/config/pipeline_config.json") as f:
    pipeline_config = json.load(f)

with open(f"{REPO_ROOT}/config/datasets.json") as f:
    datasets_config = json.load(f)

# Resolve fully qualified table name
delta_cfg = pipeline_config["delta_table"]
TABLE_NAME = f"{delta_cfg['catalog']}.{delta_cfg['schema']}.{delta_cfg['table_name']}"

api_cfg = pipeline_config["api"]
alert_cfg = pipeline_config["alerting"]
extraction_cfg = pipeline_config.get("extraction", {})
incremental_enabled = extraction_cfg.get("incremental", True)

logger.info("Pipeline configuration loaded.")
logger.info("Target Delta table: %s", TABLE_NAME)
logger.info("Incremental extraction: %s", "enabled" if incremental_enabled else "disabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Authenticate with Power BI API

# COMMAND ----------

# Read credentials from Databricks Secrets
secrets_cfg = pipeline_config["secrets"]
tenant_id = dbutils.secrets.get(scope=secrets_cfg["scope"], key=secrets_cfg["tenant_id_key"])
client_id = dbutils.secrets.get(scope=secrets_cfg["scope"], key=secrets_cfg["client_id_key"])
client_secret = dbutils.secrets.get(scope=secrets_cfg["scope"], key=secrets_cfg["client_secret_key"])

# Acquire OAuth2 token
authenticator = PowerBIAuthenticator(tenant_id, client_id, client_secret)
access_token = authenticator.get_access_token()
logger.info("Successfully acquired Power BI access token.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Resolve Dataset Registry (Discovery)

# COMMAND ----------

client = PowerBIClient(
    access_token=access_token,
    max_retries=api_cfg["max_retries"],
    backoff_factor=api_cfg["backoff_factor"],
    request_timeout=api_cfg["request_timeout_seconds"],
)

resolved_datasets = resolve_dataset_registry(
    client=client,
    datasets_config=datasets_config,
    inter_request_delay=api_cfg.get("inter_request_delay_seconds", 0.2),
)

logger.info("Resolved %d active datasets to process.", len(resolved_datasets))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load Watermarks for Incremental Extraction

# COMMAND ----------

ensure_table_exists(spark, TABLE_NAME)

if incremental_enabled:
    watermarks = get_watermarks(spark, TABLE_NAME)
    logger.info("Loaded watermarks for %d datasets.", len(watermarks))
else:
    watermarks = {}
    logger.info("Full extraction mode — no watermarks applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Extract Refresh Histories (Concurrent + Incremental)

# COMMAND ----------

max_workers = api_cfg.get("max_concurrent_requests", 4)
top_records = api_cfg["top_records_per_dataset"]

stats = {"datasets_processed": 0, "datasets_skipped": 0, "datasets_failed": 0}


def extract_dataset(dataset_entry: dict) -> list[dict]:
    """Extract and transform refresh records for a single dataset."""
    ws_id = dataset_entry["workspace_id"]
    ds_id = dataset_entry["dataset_id"]
    ds_name = dataset_entry["dataset_name"]
    ws_name = dataset_entry["workspace_name"]
    is_critical = dataset_entry.get("is_critical", False)
    wm = watermarks.get(ds_id)

    raw_records = client.get_refresh_history_safe(
        workspace_id=ws_id,
        dataset_id=ds_id,
        dataset_name=ds_name,
        top=top_records,
    )

    if not raw_records:
        return []

    filtered = filter_records_by_watermark(raw_records, wm)

    if not filtered:
        logger.info("  '%s': all %d records already known (watermark: %s). Skipping.",
                     ds_name, len(raw_records), wm)
        return []

    transformed = transform_refresh_records(
        raw_records=filtered,
        dataset_id=ds_id,
        dataset_name=ds_name,
        workspace_id=ws_id,
        workspace_name=ws_name,
        is_critical=is_critical,
    )

    logger.info("  '%s': %d new of %d fetched (watermark: %s).",
                 ds_name, len(transformed), len(raw_records), wm or "none")
    return transformed


all_records = []

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_entry = {
        executor.submit(extract_dataset, entry): entry
        for entry in resolved_datasets
    }

    for future in as_completed(future_to_entry):
        entry = future_to_entry[future]
        try:
            records = future.result()
            if records:
                all_records.extend(records)
                stats["datasets_processed"] += 1
            else:
                stats["datasets_skipped"] += 1
        except Exception as exc:
            stats["datasets_failed"] += 1
            logger.error(
                "Failed to extract dataset '%s' (%s): %s",
                entry.get("dataset_name", "?"),
                entry.get("dataset_id", "?"),
                exc,
            )

logger.info(
    "Extraction complete: %d records from %d datasets (%d skipped, %d failed).",
    len(all_records),
    stats["datasets_processed"],
    stats["datasets_skipped"],
    stats["datasets_failed"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Load into Delta Table

# COMMAND ----------

if all_records:
    # Create a Spark DataFrame from the transformed records
    new_data_df = spark.createDataFrame(all_records, schema=REFRESH_HISTORY_SCHEMA)
    logger.info("Created DataFrame with %d rows. Schema:", new_data_df.count())
    new_data_df.printSchema()

    # Upsert into the Delta table
    upsert_refresh_data(spark, new_data_df, TABLE_NAME)
    logger.info("Delta table '%s' updated successfully.", TABLE_NAME)
else:
    logger.warning("No new records to load — all datasets were up-to-date or returned empty results.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Post-Ingestion Alert Checks

# COMMAND ----------

if alert_cfg.get("enabled", False) and all_records:
    dispatcher = AlertDispatcher(teams_webhook_url=alert_cfg.get("teams_webhook_url"))

    all_alerts = []

    if alert_cfg.get("check_failures", True):
        failure_alerts = check_refresh_failures(spark, TABLE_NAME, dispatcher)
        all_alerts.extend(failure_alerts)

    if alert_cfg.get("check_duration_anomalies", True):
        anomaly_alerts = check_duration_anomalies(
            spark,
            TABLE_NAME,
            dispatcher,
            threshold_multiplier=alert_cfg.get("duration_anomaly_threshold_multiplier", 1.5),
            rolling_window_days=alert_cfg.get("duration_anomaly_rolling_window_days", 7),
        )
        all_alerts.extend(anomaly_alerts)

    if alert_cfg.get("check_stale_data", True):
        stale_alerts = check_stale_data(
            spark,
            TABLE_NAME,
            dispatcher,
            staleness_threshold_hours=alert_cfg.get("staleness_threshold_hours", 26),
        )
        all_alerts.extend(stale_alerts)

    logger.info("Alert checks complete. Total alerts fired: %d", len(all_alerts))
else:
    logger.info("Alerting is disabled or no new records were ingested — skipping alert checks.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Cleanup & Summary

# COMMAND ----------

client.close()

print("=" * 60)
print("  PIPELINE RUN SUMMARY")
print("=" * 60)
print(f"  Datasets resolved:   {len(resolved_datasets)}")
print(f"  Datasets processed:  {stats['datasets_processed']}")
print(f"  Datasets skipped:    {stats['datasets_skipped']} (up-to-date)")
print(f"  Datasets failed:     {stats['datasets_failed']}")
print(f"  Records ingested:    {len(all_records)}")
print(f"  Target table:        {TABLE_NAME}")
print(f"  Incremental:         {'yes' if incremental_enabled else 'no'}")
if alert_cfg.get("enabled", False) and all_records:
    print(f"  Alerts fired:        {len(all_alerts)}")
print("=" * 60)
