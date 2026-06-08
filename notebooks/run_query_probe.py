# Databricks notebook source

# MAGIC %md
# MAGIC # Power BI Response-Time Probe — Daily Measurement Pipeline
# MAGIC
# MAGIC Actively measures dataset response time by running a representative DAX query
# MAGIC against each configured dataset via the **Execute Queries** REST API, timing the
# MAGIC round trip, and appending the result to a Delta table.
# MAGIC
# MAGIC This is a **near-zero-cost alternative to Azure Log Analytics** for response-time
# MAGIC trending — cost is just a few seconds of Databricks compute per run, with no
# MAGIC per-GB ingestion charges.
# MAGIC
# MAGIC **Features:**
# MAGIC - **Identical probe each run** — clean apples-to-apples trend line per dataset.
# MAGIC - **Concurrent probing** — configurable thread pool (`api.max_concurrent_requests`).
# MAGIC - **Partial-failure tolerance** — a failing probe is recorded as `Failed`, not fatal.
# MAGIC
# MAGIC **Schedule:** Daily (or more frequently for finer trends) via Databricks Workflow.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Service Principal with dataset access + the **"Dataset Execute Queries REST API"**
# MAGIC   tenant setting enabled (a permission toggle — no extra cost).
# MAGIC - Credentials in Databricks Secret Scope `powerbi-monitor`.
# MAGIC - `config/query_probes.json` populated.

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
logger = logging.getLogger("powerbi_query_probe")

# Add the repo root to the Python path so local modules are importable.
REPO_ROOT = "/Workspace/Repos/<user>/powerbi-refresh-monitor"
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.auth import PowerBIAuthenticator
from src.api_client import PowerBIClient
from src.query_probe import measure_query, get_active_probes
from src.delta_ops import (
    ensure_query_performance_table_exists,
    append_query_performance_data,
    QUERY_PERFORMANCE_SCHEMA,
)

# COMMAND ----------

# Load configuration
with open(f"{REPO_ROOT}/config/pipeline_config.json") as f:
    pipeline_config = json.load(f)

with open(f"{REPO_ROOT}/config/query_probes.json") as f:
    probes_config = json.load(f)

qp_cfg = pipeline_config["query_performance_table"]
TABLE_NAME = f"{qp_cfg['catalog']}.{qp_cfg['schema']}.{qp_cfg['table_name']}"

api_cfg = pipeline_config["api"]

logger.info("Configuration loaded. Target table: %s", TABLE_NAME)

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
# MAGIC ## 3. Run Probes (Concurrent)

# COMMAND ----------

client = PowerBIClient(
    access_token=access_token,
    max_retries=api_cfg["max_retries"],
    backoff_factor=api_cfg["backoff_factor"],
    request_timeout=api_cfg["request_timeout_seconds"],
)

active_probes = get_active_probes(probes_config)
logger.info("Running %d active probes.", len(active_probes))

max_workers = api_cfg.get("max_concurrent_requests", 4)
records = []

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {executor.submit(measure_query, client, probe): probe for probe in active_probes}
    for future in as_completed(futures):
        # measure_query never raises — it always returns a record.
        records.append(future.result())

success = sum(1 for r in records if r["status"] == "Success")
failed = len(records) - success
logger.info("Probing complete: %d succeeded, %d failed.", success, failed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Append Measurements to Delta Table

# COMMAND ----------

ensure_query_performance_table_exists(spark, TABLE_NAME)

if records:
    df = spark.createDataFrame(records, schema=QUERY_PERFORMANCE_SCHEMA)
    append_query_performance_data(spark, df, TABLE_NAME)
    logger.info("Appended %d measurements to '%s'.", len(records), TABLE_NAME)
else:
    logger.warning("No probes ran — nothing to append.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary

# COMMAND ----------

client.close()

print("=" * 60)
print("  QUERY PROBE RUN SUMMARY")
print("=" * 60)
print(f"  Probes run:       {len(records)}")
print(f"  Succeeded:        {success}")
print(f"  Failed:           {failed}")
print(f"  Target table:     {TABLE_NAME}")
print("=" * 60)
