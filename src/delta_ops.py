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


QUERY_PERFORMANCE_SCHEMA = StructType(
    [
        StructField("probe_id", StringType(), nullable=False),
        StructField("workspace_id", StringType(), nullable=False),
        StructField("workspace_name", StringType(), nullable=True),
        StructField("dataset_id", StringType(), nullable=False),
        StructField("dataset_name", StringType(), nullable=True),
        StructField("query_label", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("duration_ms", DoubleType(), nullable=True),
        StructField("row_count", IntegerType(), nullable=True),
        StructField("error_message", StringType(), nullable=True),
        StructField("is_critical", BooleanType(), nullable=True),
        StructField("probe_timestamp", TimestampType(), nullable=False),
        StructField("probe_date", DateType(), nullable=False),
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


def get_query_performance_ddl(table_name: str) -> str:
    """Return the DDL for the query-performance (response-time) Delta table."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        probe_id            STRING          NOT NULL
            COMMENT 'Unique identifier per probe measurement',
        workspace_id        STRING          NOT NULL
            COMMENT 'Power BI workspace (group) ID',
        workspace_name      STRING
            COMMENT 'Human-readable workspace name from config',
        dataset_id          STRING          NOT NULL
            COMMENT 'Power BI dataset (semantic model) ID',
        dataset_name        STRING
            COMMENT 'Human-readable dataset name from config',
        query_label         STRING
            COMMENT 'Label of the representative probe query',
        status              STRING
            COMMENT 'Success | Failed',
        duration_ms         DOUBLE
            COMMENT 'Measured round-trip query duration in milliseconds',
        row_count           INT
            COMMENT 'Number of rows returned by the probe query',
        error_message       STRING
            COMMENT 'Error detail when status = Failed (NULL otherwise)',
        is_critical         BOOLEAN
            COMMENT 'Whether this dataset is flagged as business-critical',
        probe_timestamp     TIMESTAMP       NOT NULL
            COMMENT 'UTC timestamp when the probe ran',
        probe_date          DATE            NOT NULL
            COMMENT 'Partition column: date the probe ran'
    )
    USING DELTA
    COMMENT 'Active response-time probe measurements for Power BI datasets (Execute Queries API).'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
    """


def ensure_query_performance_table_exists(spark: SparkSession, table_name: str) -> None:
    """Create the query-performance Delta table if it does not already exist."""
    logger.info("Ensuring query-performance table '%s' exists.", table_name)
    spark.sql(get_query_performance_ddl(table_name))
    logger.info("Query-performance table '%s' is ready.", table_name)


def append_query_performance_data(
    spark: SparkSession,
    new_data_df: DataFrame,
    table_name: str,
) -> None:
    """Append probe measurements to the query-performance Delta table.

    Each probe is a fresh measurement (unique ``probe_id``), so records are
    appended rather than merged.

    Parameters
    ----------
    spark : SparkSession
    new_data_df : DataFrame
        Transformed probe records to append.
    table_name : str
        Fully qualified Delta table name.
    """
    if new_data_df.isEmpty():
        logger.info("No probe measurements to append.")
        return

    row_count = new_data_df.count()
    logger.info("Appending %d probe measurements into '%s'.", row_count, table_name)
    new_data_df.write.format("delta").mode("append").saveAsTable(table_name)
    logger.info("Append complete for '%s'.", table_name)


ACTIVITY_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("creation_time", TimestampType(), nullable=True),
        StructField("creation_date", DateType(), nullable=True),
        StructField("activity", StringType(), nullable=True),
        StructField("user_id", StringType(), nullable=True),
        StructField("user_key", StringType(), nullable=True),
        StructField("workspace_id", StringType(), nullable=True),
        StructField("workspace_name", StringType(), nullable=True),
        StructField("report_id", StringType(), nullable=True),
        StructField("report_name", StringType(), nullable=True),
        StructField("report_type", StringType(), nullable=True),
        StructField("dataset_id", StringType(), nullable=True),
        StructField("dataset_name", StringType(), nullable=True),
        StructField("capacity_id", StringType(), nullable=True),
        StructField("consumption_method", StringType(), nullable=True),
        StructField("distribution_method", StringType(), nullable=True),
        StructField("item_name", StringType(), nullable=True),
        StructField("object_id", StringType(), nullable=True),
        StructField("result_status", StringType(), nullable=True),
        StructField("ingestion_timestamp", TimestampType(), nullable=False),
        StructField("ingestion_date", DateType(), nullable=False),
    ]
)


def get_activity_events_ddl(table_name: str) -> str:
    """Return the DDL for the activity-events (usage) Delta table."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_id            STRING          NOT NULL
            COMMENT 'Unique identifier per audit event (from Power BI API)',
        creation_time       TIMESTAMP
            COMMENT 'UTC time the activity occurred',
        creation_date       DATE
            COMMENT 'Partition column: date the activity occurred (UTC)',
        activity            STRING
            COMMENT 'Activity type, e.g. ViewReport | ViewDashboard | CreateReport',
        user_id             STRING
            COMMENT 'UPN / email of the user who performed the activity',
        user_key            STRING
            COMMENT 'Stable internal key for the user',
        workspace_id        STRING
            COMMENT 'Power BI workspace (group) ID, when applicable',
        workspace_name      STRING
            COMMENT 'Workspace name, when applicable',
        report_id           STRING
            COMMENT 'Report ID, when the activity targets a report',
        report_name         STRING
            COMMENT 'Report name, when applicable',
        report_type         STRING
            COMMENT 'Report type, e.g. PowerBIReport | PaginatedReport',
        dataset_id          STRING
            COMMENT 'Dataset (semantic model) ID, when applicable',
        dataset_name        STRING
            COMMENT 'Dataset name, when applicable',
        capacity_id         STRING
            COMMENT 'Capacity ID hosting the item, when applicable',
        consumption_method  STRING
            COMMENT 'How the content was consumed, when reported',
        distribution_method STRING
            COMMENT 'How the content was distributed, when reported',
        item_name           STRING
            COMMENT 'Name of the affected item, when reported',
        object_id           STRING
            COMMENT 'Object ID of the affected item, when reported',
        result_status       STRING
            COMMENT 'Succeeded | Failed for the activity, when reported',
        ingestion_timestamp TIMESTAMP       NOT NULL
            COMMENT 'UTC timestamp when this record was ingested by the pipeline',
        ingestion_date      DATE            NOT NULL
            COMMENT 'Date this record was ingested'
    )
    USING DELTA
    PARTITIONED BY (creation_date)
    COMMENT 'Power BI activity (audit) events for usage/popularity analysis, ingested daily.'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
    """


def ensure_activity_events_table_exists(spark: SparkSession, table_name: str) -> None:
    """Create the activity-events Delta table if it does not already exist."""
    logger.info("Ensuring activity-events table '%s' exists.", table_name)
    spark.sql(get_activity_events_ddl(table_name))
    logger.info("Activity-events table '%s' is ready.", table_name)


def get_max_activity_date(spark: SparkSession, table_name: str):
    """Return the latest ``creation_date`` already stored, or None.

    Used to drive incremental day-by-day extraction so we re-fetch only the
    most recent (possibly incomplete) days rather than the full window.

    Returns
    -------
    datetime.date | None
        The max stored activity date, or None if the table is absent/empty.
    """
    try:
        if not spark.catalog.tableExists(table_name):
            return None
        row = spark.sql(
            f"SELECT MAX(creation_date) AS max_date FROM {table_name}"
        ).collect()[0]
        return row["max_date"]
    except Exception as exc:
        logger.warning(
            "Could not read activity watermark from '%s': %s. Using full window.",
            table_name,
            exc,
        )
        return None


def upsert_activity_events(
    spark: SparkSession,
    new_data_df: DataFrame,
    table_name: str,
) -> None:
    """Merge new activity events into the Delta table, keyed on ``event_id``.

    Audit events are immutable, so matched rows are left untouched and only
    new ``event_id`` values are inserted. This makes re-running a day (e.g.
    overlapping windows) idempotent — no duplicates.

    Parameters
    ----------
    spark : SparkSession
    new_data_df : DataFrame
        Transformed activity-event records to merge.
    table_name : str
        Fully qualified Delta table name.
    """
    from delta.tables import DeltaTable

    if new_data_df.isEmpty():
        logger.info("No new activity events to merge.")
        return

    deduped_df = new_data_df.dropDuplicates(["event_id"])
    row_count = deduped_df.count()
    logger.info("Merging %d activity events into '%s'.", row_count, table_name)

    delta_table = DeltaTable.forName(spark, table_name)
    delta_table.alias("target").merge(
        deduped_df.alias("source"),
        "target.event_id = source.event_id",
    ).whenNotMatchedInsertAll().execute()

    logger.info("Activity-events merge complete for '%s'.", table_name)
