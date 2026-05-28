# Databricks notebook source

# MAGIC %md
# MAGIC # Delta Table Setup — Power BI Refresh History
# MAGIC
# MAGIC **Run this notebook once** to create the Delta table and verify the schema.
# MAGIC This is idempotent — it uses `CREATE TABLE IF NOT EXISTS`.

# COMMAND ----------

import json
import sys

REPO_ROOT = "/Workspace/Repos/<user>/powerbi-refresh-monitor"
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.delta_ops import ensure_table_exists, get_create_table_ddl

# COMMAND ----------

# Load configuration
with open(f"{REPO_ROOT}/config/pipeline_config.json") as f:
    config = json.load(f)

delta_cfg = config["delta_table"]
TABLE_NAME = f"{delta_cfg['catalog']}.{delta_cfg['schema']}.{delta_cfg['table_name']}"

print(f"Target table: {TABLE_NAME}")
print()
print("DDL to execute:")
print(get_create_table_ddl(TABLE_NAME))

# COMMAND ----------

ensure_table_exists(spark, TABLE_NAME)

# COMMAND ----------

# Verify the table was created
df = spark.sql(f"DESCRIBE TABLE EXTENDED {TABLE_NAME}")
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Access (adjust group names to match your organization)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data Engineers: full access
# MAGIC -- GRANT SELECT, MODIFY ON TABLE your_catalog.your_schema.powerbi_refresh_history
# MAGIC -- TO `data-engineering-group`;
# MAGIC
# MAGIC -- Analysts: read-only
# MAGIC -- GRANT SELECT ON TABLE your_catalog.your_schema.powerbi_refresh_history
# MAGIC -- TO `data-analysts-group`;
