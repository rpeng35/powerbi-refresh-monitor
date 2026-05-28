# Databricks notebook source

# MAGIC %md
# MAGIC # Power BI Refresh History — Daily Ingestion Pipeline
# MAGIC
# MAGIC Extracts refresh metadata from the Power BI REST API for all configured datasets,
# MAGIC transforms the data, and upserts into a Delta Lake table. Optionally runs
# MAGIC post-ingestion alert checks.
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
import time

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
from src.transform import transform_refresh_records
from src.delta_ops import ensure_table_exists, upsert_refresh_data, REFRESH_HISTORY_SCHEMA
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

active_datasets = [d for d in datasets_config["datasets"] if d.get("is_active", True)]

logger.info("Pipeline configuration loaded.")
logger.info("Target Delta table: %s", TABLE_NAME)
logger.info("Active datasets to process: %d", len(active_datasets))

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
# MAGIC ## 3. Extract Refresh Histories

# COMMAND ----------

all_records = []

with PowerBIClient(
    access_token=access_token,
    max_retries=api_cfg["max_retries"],
    backoff_factor=api_cfg["backoff_factor"],
    request_timeout=api_cfg["request_timeout_seconds"],
) as client:
    for dataset_entry in active_datasets:
        ws_id = dataset_entry["workspace_id"]
        ds_id = dataset_entry["dataset_id"]
        ds_name = dataset_entry["dataset_name"]
        ws_name = dataset_entry["workspace_name"]
        is_critical = dataset_entry.get("is_critical", False)

        logger.info("Fetching refresh history for '%s' (workspace: '%s')...", ds_name, ws_name)

        raw_records = client.get_refresh_history_safe(
            workspace_id=ws_id,
            dataset_id=ds_id,
            dataset_name=ds_name,
            top=api_cfg["top_records_per_dataset"],
        )

        if raw_records:
            transformed = transform_refresh_records(
                raw_records=raw_records,
                dataset_id=ds_id,
                dataset_name=ds_name,
                workspace_id=ws_id,
                workspace_name=ws_name,
                is_critical=is_critical,
            )
            all_records.extend(transformed)
            logger.info("  -> %d refresh records extracted and transformed.", len(transformed))
        else:
            logger.info("  -> No refresh records returned.")

        # Respect API rate limits
        time.sleep(api_cfg.get("inter_request_delay_seconds", 0.2))

logger.info("Total records extracted across all datasets: %d", len(all_records))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load into Delta Table

# COMMAND ----------

# Ensure the target table exists
ensure_table_exists(spark, TABLE_NAME)

if all_records:
    # Create a Spark DataFrame from the transformed records
    new_data_df = spark.createDataFrame(all_records, schema=REFRESH_HISTORY_SCHEMA)
    logger.info("Created DataFrame with %d rows. Schema:", new_data_df.count())
    new_data_df.printSchema()

    # Upsert into the Delta table
    upsert_refresh_data(spark, new_data_df, TABLE_NAME)
    logger.info("Delta table '%s' updated successfully.", TABLE_NAME)
else:
    logger.warning("No records to load — all API calls returned empty results.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Post-Ingestion Alert Checks

# COMMAND ----------

if alert_cfg.get("enabled", False) and all_records:
    dispatcher = AlertDispatcher(slack_webhook_url=alert_cfg.get("slack_webhook_url"))

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
    logger.info("Alerting is disabled or no records were ingested — skipping alert checks.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary

# COMMAND ----------

print("=" * 60)
print("  PIPELINE RUN SUMMARY")
print("=" * 60)
print(f"  Datasets processed:  {len(active_datasets)}")
print(f"  Records ingested:    {len(all_records)}")
print(f"  Target table:        {TABLE_NAME}")
if alert_cfg.get("enabled", False) and all_records:
    print(f"  Alerts fired:        {len(all_alerts)}")
print("=" * 60)
