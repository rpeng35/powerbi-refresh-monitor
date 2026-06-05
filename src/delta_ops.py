"""
Delta Lake operations for the Power BI refresh history table.

Provides schema definition, table creation, and upsert (MERGE) logic.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

REFRESH_HISTORY_SCHEMA = StructType(
    [
        StructField("request_id", StringType(), nullable=False),
        StructField("dataset_id", StringType(), nullable=False),
        StructField("dataset_name", StringType(), nullable=True),
        StructField("workspace_id", StringType(), nullable=False),
        StructField("workspace_name", StringType(), nullable=True),
        StructField("refresh_type", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("start_time", TimestampType(), nullable=True),
        StructField("end_time", TimestampType(), nullable=True),
        StructField("duration_seconds", DoubleType(), nullable=True),
        StructField("service_exception_json", StringType(), nullable=True),
        StructField("error_code", StringType(), nullable=True),
        StructField("refresh_attempts_json", StringType(), nullable=True),
        StructField("attempt_count", IntegerType(), nullable=True),
        StructField("is_critical", BooleanType(), nullable=True),
        StructField("ingestion_timestamp", TimestampType(), nullable=False),
        StructField("ingestion_date", DateType(), nullable=False),
    ]
)


def get_watermarks(spark: SparkSession, table_name: str) -> dict[str, str]:
    """Return the latest start_time per dataset_id from the Delta table.

    Used for incremental extraction — only records newer than the watermark
    need to be processed.

    Parameters
    ----------
    spark : SparkSession
    table_name : str
        Fully qualified Delta table name.

    Returns
    -------
    dict[str, str]
        Mapping of dataset_id to latest start_time (ISO-8601 string).
        Empty dict if the table doesn't exist or has no data.
    """
    try:
        rows = spark.sql(
            f"SELECT dataset_id, MAX(start_time) AS latest "
            f"FROM {table_name} "
            f"WHERE start_time IS NOT NULL "
            f"GROUP BY dataset_id"
        ).collect()
        return {
            row.dataset_id: row.latest.isoformat()
            for row in rows
            if row.latest is not None
        }
    except Exception as exc:
        logger.warning(
            "Could not read watermarks from '%s': %s. Running full extraction.",
            table_name,
            exc,
        )
        return {}


def get_create_table_ddl(table_name: str) -> str:
    """Return the DDL statement for creating the refresh history Delta table."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        request_id              STRING          NOT NULL
            COMMENT 'Unique identifier per refresh attempt (from Power BI API)',
        dataset_id              STRING          NOT NULL
            COMMENT 'Power BI dataset (semantic model) ID',
        dataset_name            STRING
            COMMENT 'Human-readable dataset name from config table',
        workspace_id            STRING          NOT NULL
            COMMENT 'Power BI workspace (group) ID',
        workspace_name          STRING
            COMMENT 'Human-readable workspace name from config table',
        refresh_type            STRING
            COMMENT 'Scheduled | ViaApi | OnDemand | ViaEnhancedApi',
        status                  STRING
            COMMENT 'Completed | Failed | Unknown | Disabled | Cancelled',
        start_time              TIMESTAMP
            COMMENT 'Refresh start time (UTC)',
        end_time                TIMESTAMP
            COMMENT 'Refresh end time (UTC); NULL if in-progress',
        duration_seconds        DOUBLE
            COMMENT 'Computed: endTime - startTime in seconds; NULL if in-progress',
        service_exception_json  STRING
            COMMENT 'Raw JSON error payload from Power BI (NULL if no error)',
        error_code              STRING
            COMMENT 'Extracted error code from serviceExceptionJson',
        refresh_attempts_json   STRING
            COMMENT 'Raw JSON array of refresh attempt details',
        attempt_count           INT
            COMMENT 'Number of sub-attempts within this refresh',
        is_critical             BOOLEAN
            COMMENT 'Whether this dataset is flagged as business-critical',
        ingestion_timestamp     TIMESTAMP       NOT NULL
            COMMENT 'UTC timestamp when this record was ingested/updated by the pipeline',
        ingestion_date          DATE            NOT NULL
            COMMENT 'Partition column: date of ingestion'
    )
    USING DELTA
    COMMENT 'Historical Power BI semantic model refresh metadata, ingested daily via automated pipeline.'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
    """


def ensure_table_exists(spark: SparkSession, table_name: str) -> None:
    """Create the Delta table if it does not already exist."""
    logger.info("Ensuring Delta table '%s' exists.", table_name)
    spark.sql(get_create_table_ddl(table_name))
    logger.info("Delta table '%s' is ready.", table_name)


def upsert_refresh_data(spark: SparkSession, new_data_df: DataFrame, table_name: str) -> None:
    """Merge new refresh records into the historical Delta table.

    - Matching records (by ``request_id``) are updated only if the incoming
      status is no longer ``Unknown`` (i.e. the refresh has completed since
      the last ingestion).
    - Non-matching records are inserted.

    Parameters
    ----------
    spark : SparkSession
    new_data_df : DataFrame
        Transformed refresh records to merge.
    table_name : str
        Fully qualified Delta table name (e.g. ``catalog.schema.table``).
    """
    from delta.tables import DeltaTable

    if new_data_df.isEmpty():
        logger.info("No new records to upsert.")
        return

    row_count = new_data_df.count()
    logger.info("Upserting %d records into '%s'.", row_count, table_name)

    delta_table = DeltaTable.forName(spark, table_name)

    delta_table.alias("target").merge(
        new_data_df.alias("source"),
        "target.request_id = source.request_id",
    ).whenMatchedUpdate(
        condition="source.status != 'Unknown'",
        set={
            "status": "source.status",
            "end_time": "source.end_time",
            "duration_seconds": "source.duration_seconds",
            "service_exception_json": "source.service_exception_json",
            "error_code": "source.error_code",
            "refresh_attempts_json": "source.refresh_attempts_json",
            "attempt_count": "source.attempt_count",
            "ingestion_timestamp": "source.ingestion_timestamp",
        },
    ).whenNotMatchedInsertAll().execute()

    logger.info("Upsert complete for '%s'.", table_name)
