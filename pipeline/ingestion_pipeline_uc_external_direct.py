"""
UC External Table ingestion pipeline — serverless-compatible.

Writes EXTERNAL Delta tables in Unity Catalog without requiring
PySpark DataSource API (no SimpleDataSourceStreamReader needed).
Works on serverless, DBR 12.x, 13.x, and any newer runtime.

Key differences from ingestion_pipeline_uc_external.py:
- Calls LakeflowConnect directly instead of using Spark DataSource
- CDC offsets stored in _lakeflow_offsets Delta table (no DBFS/checkpoints)
- Compatible with serverless compute

Key differences from ingestion_pipeline_direct.py (managed):
- Each table written to <external_location_base>/<table_name>/
- saveAsTable() + path option → EXTERNAL table in UC
- DROP TABLE does NOT delete the data in cloud storage

Requirements:
- destination_catalog and destination_schema must exist (or be created here)
- external_location_base must point to a path under a registered UC External Location
"""

import json
import time
from typing import Dict, Iterator, List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


# ---------------------------------------------------------------------------
# Offset persistence via UC Delta table (no DBFS required)
# ---------------------------------------------------------------------------

_OFFSET_TABLE = "_lakeflow_offsets"


def _offset_table_name(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.`{_OFFSET_TABLE}`"


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
            SELECT '{table_name}'      AS table_name,
                   '{escaped}'         AS offset,
                   current_timestamp() AS updated_at
        ) AS s
        ON t.table_name = s.table_name
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# ---------------------------------------------------------------------------
# DataFrame creation
# ---------------------------------------------------------------------------

def _records_to_df(
    spark: SparkSession, records: Iterator[dict], schema: StructType
) -> DataFrame:
    rows = list(records)
    if not rows:
        return spark.createDataFrame([], schema=schema)
    return spark.createDataFrame(rows, schema=schema)


# ---------------------------------------------------------------------------
# External table write helpers
# ---------------------------------------------------------------------------

def _write_external(
    df: DataFrame, full_table_name: str, location: str, mode: str = "overwrite"
) -> None:
    """Write DataFrame as an EXTERNAL Delta table at `location` registered in UC."""
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("path", location)
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )


def _merge_external(
    spark: SparkSession, df: DataFrame, full_table_name: str, pks: List[str],
    max_retries: int = 5,
) -> None:
    """MERGE with exponential-backoff retry on ConcurrentAppendException.

    Delta raises ConcurrentAppendException when concurrent MERGEs on the same
    table conflict under WriteSerializable isolation. The exception is explicitly
    retryable — the conflicting commit has already finished, so a fresh attempt
    reads the new table state and succeeds.
    """
    merge_cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in pks)
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


# ---------------------------------------------------------------------------
# Ingestion strategies
# ---------------------------------------------------------------------------

def _ingest_cdc(
    spark: SparkSession,
    connector,
    source_table: str,
    full_dest: str,
    location: str,
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
        if spark.catalog.tableExists(full_dest):
            _merge_external(spark, df, full_dest, primary_keys)
        else:
            _write_external(df, full_dest, location)

    _save_offset(spark, full_offset_table, source_table, new_offset)


def _ingest_snapshot(
    spark: SparkSession,
    connector,
    source_table: str,
    full_dest: str,
    location: str,
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
        _merge_external(spark, df, full_dest, primary_keys)
    else:
        _write_external(df, full_dest, location, mode="overwrite")


# ---------------------------------------------------------------------------
# Metadata helper
# ---------------------------------------------------------------------------

def _get_metadata_direct(connector, table_list: List[str], table_configs: Dict) -> Dict:
    metadata = {}
    for table in table_list:
        try:
            metadata[table] = connector.read_table_metadata(
                table, table_configs.get(table, {})
            )
        except Exception:
            metadata[table] = {}
    return metadata


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

_PIPELINE_KEYS = {"scd_type", "primary_keys", "sequence_by"}


def ingest_uc_external_direct(spark: SparkSession, pipeline_spec: dict) -> None:
    """
    Ingest tables from a Lakeflow community connector into Unity Catalog
    as EXTERNAL Delta tables. Serverless-compatible — no DBFS required.

    Parameters
    ----------
    spark : SparkSession
    pipeline_spec : dict
        {
            "api_token": "<monday_token>",            # required
            "destination_catalog": "my_catalog",      # required
            "destination_schema": "monday_external",  # required
            "external_location_base": "s3://bucket/monday",  # required
                # Must be under a registered UC External Location.
            "objects": [
                {
                    "table": {
                        "source_table": "boards",
                        "destination_table": "boards",    # optional
                        "table_configuration": {
                            "state": "active",
                            "scd_type": "SCD_TYPE_1",
                        }
                    }
                },
                ...
            ]
        }

    Storage layout
    --------------
    <external_location_base>/<dest_table_name>/     ← Delta table data
    <destination_schema>._lakeflow_offsets          ← CDC offset tracking (managed table)

    Unity Catalog requirements
    --------------------------
    * destination_catalog must exist.
    * destination_schema is created automatically if absent.
    * external_location_base must be under a registered External Location.
    * The running identity needs CREATE TABLE + WRITE FILES on the schema/location.
    """
    from sources.monday.monday import LakeflowConnect

    api_token: str = pipeline_spec.get("api_token", "")
    if not api_token:
        raise ValueError("'api_token' is required")

    dest_catalog: str = pipeline_spec.get("destination_catalog", "")
    if not dest_catalog:
        raise ValueError("'destination_catalog' is required")

    dest_schema: str = pipeline_spec.get("destination_schema", "")
    if not dest_schema:
        raise ValueError("'destination_schema' is required")

    ext_base: str = pipeline_spec.get("external_location_base", "").rstrip("/")
    if not ext_base:
        raise ValueError("'external_location_base' is required")

    objects: list = pipeline_spec.get("objects", [])
    if not objects:
        raise ValueError("'objects' must be a non-empty list")

    # Ensure schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{dest_catalog}`.`{dest_schema}`")

    full_offset_table = _offset_table_name(dest_catalog, dest_schema)

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

        effective_catalog = tspec.get("destination_catalog") or dest_catalog
        effective_schema  = tspec.get("destination_schema")  or dest_schema
        full_dest = f"`{effective_catalog}`.`{effective_schema}`.`{dest_table_name}`"
        location  = f"{ext_base}/{dest_table_name}"

        print(f"[lakeflow-uc-ext-direct] {source_table} → {full_dest}")
        print(f"  location: {location} [{ingestion_type}]")

        if ingestion_type in ("cdc", "cdc_with_deletes"):
            _ingest_cdc(
                spark, connector, source_table, full_dest, location,
                src_options, primary_keys, full_offset_table
            )
        else:
            _ingest_snapshot(spark, connector, source_table, full_dest, location, src_options, primary_keys)

        print(f"  done")
