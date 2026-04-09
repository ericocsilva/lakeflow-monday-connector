"""
Direct ingestion pipeline for Lakeflow Community Connectors.

Compatible with ANY Databricks Runtime — including serverless (no DBFS needed).

Instead of registering a PySpark DataSource (which requires DBR 14.3+
for SimpleDataSourceStreamReader), this pipeline calls LakeflowConnect
directly and creates DataFrames via spark.createDataFrame().

CDC offsets are persisted in a UC Delta table (_lakeflow_offsets) in the
same schema as the ingested tables — no DBFS or checkpoints required.

Ingestion behaviour per table type:
  cdc      → read_table(start=last_offset) + MERGE  (boards, items)
  snapshot → read_table(start=None) + overwrite     (all other tables)
"""

import json
import time
from typing import Dict, Iterator, List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


# ---------------------------------------------------------------------------
# Offset persistence via UC Delta table (works on serverless — no DBFS)
# ---------------------------------------------------------------------------

_OFFSET_TABLE = "_lakeflow_offsets"


def _offset_table_name(dest_catalog: Optional[str], dest_schema: str) -> str:
    if dest_catalog:
        return f"`{dest_catalog}`.`{dest_schema}`.`{_OFFSET_TABLE}`"
    return f"`{dest_schema}`.`{_OFFSET_TABLE}`"


def _ensure_offset_table(spark: SparkSession, full_offset_table: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_offset_table} (
            table_name STRING NOT NULL,
            offset     STRING NOT NULL,
            updated_at TIMESTAMP
        ) USING DELTA
    """)


def _load_offset(spark: SparkSession, full_offset_table: str, table_name: str) -> dict:
    try:
        rows = spark.sql(
            f"SELECT offset FROM {full_offset_table} WHERE table_name = '{table_name}'"
        ).collect()
        if rows:
            return json.loads(rows[0]["offset"])
    except Exception:
        pass
    return {}


def _save_offset(
    spark: SparkSession, full_offset_table: str, table_name: str, offset: dict
) -> None:
    _ensure_offset_table(spark, full_offset_table)
    escaped = json.dumps(offset).replace("'", "\\'")
    spark.sql(f"""
        MERGE INTO {full_offset_table} AS t
        USING (
            SELECT '{table_name}'        AS table_name,
                   '{escaped}'           AS offset,
                   current_timestamp()   AS updated_at
        ) AS s
        ON t.table_name = s.table_name
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# ---------------------------------------------------------------------------
# DataFrame creation from connector records
# ---------------------------------------------------------------------------

def _records_to_df(
    spark: SparkSession,
    records: Iterator[dict],
    schema: StructType,
) -> DataFrame:
    """
    Convert an iterator of plain dicts (from LakeflowConnect) to a Spark DataFrame.
    spark.createDataFrame accepts dicts when a schema is provided.
    """
    rows = list(records)
    if not rows:
        return spark.createDataFrame([], schema=schema)
    return spark.createDataFrame(rows, schema=schema)


# ---------------------------------------------------------------------------
# Ingestion strategies
# ---------------------------------------------------------------------------

def _merge_df(
    spark: SparkSession, df: DataFrame, full_table_name: str, pks: List[str],
    max_retries: int = 5,
) -> None:
    """MERGE with exponential-backoff retry on ConcurrentAppendException."""
    merge_cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in pks)
    if not spark.catalog.tableExists(full_table_name):
        df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
        return
    for attempt in range(max_retries):
        try:
            (
                DeltaTable.forName(spark, full_table_name)
                .alias("t")
                .merge(df.alias("s"), merge_cond)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            return
        except Exception as e:
            if "ConcurrentAppendException" in type(e).__name__ or "ConcurrentAppendException" in str(e):
                if attempt < max_retries - 1:
                    wait = 2 ** attempt
                    print(f"  ConcurrentAppendException on {full_table_name}, retry {attempt + 1}/{max_retries - 1} in {wait}s")
                    time.sleep(wait)
                    continue
            raise


def _ingest_cdc_direct(
    spark: SparkSession,
    connector,
    source_table: str,
    full_dest: str,
    src_options: dict,
    primary_keys: List[str],
    full_offset_table: str,
) -> None:
    _ensure_offset_table(spark, full_offset_table)
    offset = _load_offset(spark, full_offset_table, source_table)
    schema = connector.get_table_schema(source_table, src_options)
    records, new_offset = connector.read_table(source_table, offset or None, src_options)
    df = _records_to_df(spark, records, schema)
    if df.count() > 0:
        _merge_df(spark, df, full_dest, primary_keys)
    _save_offset(spark, full_offset_table, source_table, new_offset)


def _ingest_snapshot_direct(
    spark: SparkSession,
    connector,
    source_table: str,
    full_dest: str,
    src_options: dict,
    primary_keys: Optional[List[str]] = None,
) -> None:
    """Ingest a snapshot table.

    When primary_keys are provided (ForEach mode), uses MERGE so that each
    board's rows are upserted without overwriting other boards' data.
    Falls back to full overwrite when no primary_keys are given (single-run mode).
    """
    schema = connector.get_table_schema(source_table, src_options)
    records, _ = connector.read_table(source_table, None, src_options)
    df = _records_to_df(spark, records, schema)
    if primary_keys and spark.catalog.tableExists(full_dest):
        _merge_df(spark, df, full_dest, primary_keys)
    else:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_dest)


# ---------------------------------------------------------------------------
# Metadata helper (direct call, no DataSource needed)
# ---------------------------------------------------------------------------

def _get_metadata_direct(connector, table_list: List[str], table_configs: Dict) -> Dict:
    metadata = {}
    for table in table_list:
        options = table_configs.get(table, {})
        try:
            md = connector.read_table_metadata(table, options)
            metadata[table] = md
        except Exception:
            metadata[table] = {}
    return metadata


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

_PIPELINE_KEYS = {"scd_type", "primary_keys", "sequence_by"}


def ingest_direct(spark: SparkSession, pipeline_spec: dict) -> None:
    """
    Ingest tables from a Lakeflow community connector by calling LakeflowConnect
    directly — no PySpark DataSource registration required.

    Compatible with DBR 12.x, 13.x, and serverless runtimes.

    Parameters
    ----------
    spark : SparkSession
    pipeline_spec : dict
        Same format as ingest_hms():
        {
            "api_token": "<monday_token>",          # required
            "target_database": "monday_db",         # required
            "destination_catalog": "my_catalog",    # optional (UC catalog)
            "checkpoint_base": "/dbfs/checkpoints/monday",  # optional
            "objects": [ {"table": {...}}, ... ]
        }
    """
    from sources.monday.monday import LakeflowConnect

    api_token: str = pipeline_spec.get("api_token", "")
    if not api_token:
        raise ValueError("'api_token' is required in pipeline_spec")

    target_database: str = pipeline_spec.get("target_database", "")
    if not target_database:
        raise ValueError("'target_database' is required in pipeline_spec")

    default_catalog: Optional[str] = pipeline_spec.get("destination_catalog")

    # Offsets stored in a Delta table inside the target schema — no DBFS needed
    full_offset_table = _offset_table_name(default_catalog, target_database)

    objects: list = pipeline_spec.get("objects", [])
    if not objects:
        raise ValueError("'objects' must be a non-empty list in pipeline_spec")

    # Create schema
    if default_catalog:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{default_catalog}`.`{target_database}`")
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{target_database}`")

    connector = LakeflowConnect({"api_token": api_token})

    table_list = [o["table"]["source_table"] for o in objects]
    table_configs_for_metadata = {
        o["table"]["source_table"]: {
            k: v
            for k, v in (o["table"].get("table_configuration") or {}).items()
            if k not in _PIPELINE_KEYS
        }
        for o in objects
    }

    metadata = _get_metadata_direct(connector, table_list, table_configs_for_metadata)

    for obj in objects:
        tspec = obj["table"]
        source_table: str = tspec["source_table"]
        dest_table_name: str = tspec.get("destination_table") or source_table
        table_cfg: dict = tspec.get("table_configuration") or {}

        md = metadata.get(source_table, {})

        primary_keys = table_cfg.get("primary_keys") or md.get("primary_keys") or ["id"]
        if isinstance(primary_keys, str):
            primary_keys = [k.strip() for k in primary_keys.split(",")]

        ingestion_type: str = md.get("ingestion_type", "snapshot")
        scd_type_raw: str = table_cfg.get("scd_type", "SCD_TYPE_1").upper()
        if scd_type_raw == "APPEND_ONLY":
            ingestion_type = "append"

        src_options = {k: v for k, v in table_cfg.items() if k not in _PIPELINE_KEYS}

        dest_catalog: Optional[str] = tspec.get("destination_catalog") or default_catalog
        dest_schema: str = tspec.get("destination_schema") or target_database
        if dest_catalog:
            full_dest = f"`{dest_catalog}`.`{dest_schema}`.`{dest_table_name}`"
        else:
            full_dest = f"`{dest_schema}`.`{dest_table_name}`"

        print(f"[lakeflow-direct] {source_table} → {full_dest}  [{ingestion_type}]")

        if ingestion_type in ("cdc", "cdc_with_deletes"):
            _ingest_cdc_direct(
                spark, connector, source_table, full_dest,
                src_options, primary_keys, full_offset_table
            )
        else:
            _ingest_snapshot_direct(spark, connector, source_table, full_dest, src_options, primary_keys)

        print(f"  done")
