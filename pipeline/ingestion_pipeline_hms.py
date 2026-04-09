"""
HMS-compatible ingestion pipeline for Lakeflow Community Connectors.

Drop-in alternative to ingestion_pipeline.py that works WITHOUT Unity Catalog.

Key differences from the original:
- No pyspark.pipelines (DLT/SDP) — uses standard Spark Structured Streaming
- No UC connection object — api_token is passed directly in options
- Writes managed Delta tables to Hive Metastore (or any catalog)
- CDC upserts done via Delta MERGE in foreachBatch
- Snapshots done via batch overwrite
- Checkpoints stored in DBFS

Ingestion behaviour per table type:
  cdc      → readStream + foreachBatch + MERGE (boards, items)
  snapshot → batch read  + overwrite           (users, workspaces, teams, …)
  append   → readStream  + append              (forced via scd_type=APPEND_ONLY)
"""

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


# ---------------------------------------------------------------------------
# Internal config dataclass
# ---------------------------------------------------------------------------

@dataclass
class _TableConfig:
    source_table: str
    destination_table: str       # fully-qualified: `schema`.`table` or `cat`.`schema`.`table`
    src_options: Dict[str, str]  # source-specific options (board_ids, page_size, …)
    primary_keys: List[str]
    sequence_by: Optional[str]
    scd_type: str                # "1" or "2"
    ingestion_type: str          # "cdc" | "snapshot" | "append"
    checkpoint_path: str


# ---------------------------------------------------------------------------
# Metadata helper
# ---------------------------------------------------------------------------

def _get_table_metadata(
    spark: SparkSession,
    api_token: str,
    table_list: List[str],
    table_configs: Dict[str, Dict],
) -> Dict[str, Dict]:
    """
    Ask the connector for primary_keys / cursor_field / ingestion_type.
    Uses the special _lakeflow_metadata virtual table — no UC connection needed.
    """
    df = (
        spark.read.format("lakeflow_connect")
        .option("api_token", api_token)
        .option("tableName", "_lakeflow_metadata")
        .option("tableNameList", ",".join(table_list))
        .option("tableConfigs", json.dumps(table_configs))
        .load()
    )

    metadata: Dict[str, Dict] = {}
    for row in df.collect():
        entry: Dict = {}
        if row["primary_keys"] is not None:
            entry["primary_keys"] = list(row["primary_keys"])
        if row["cursor_field"] is not None:
            entry["cursor_field"] = row["cursor_field"]
        if row["ingestion_type"] is not None:
            entry["ingestion_type"] = row["ingestion_type"]
        metadata[row["tableName"]] = entry
    return metadata


# ---------------------------------------------------------------------------
# Ingestion strategies
# ---------------------------------------------------------------------------

def _ingest_cdc(spark: SparkSession, api_token: str, cfg: _TableConfig) -> None:
    """
    Streaming CDC → Delta MERGE.

    On the first micro-batch the target table is created via saveAsTable (managed).
    Subsequent batches MERGE into the existing table keyed on primary_keys.
    trigger(availableNow=True) makes this behave like a scheduled batch job
    while still tracking the streaming offset in the checkpoint directory.
    """
    options = {"api_token": api_token, "tableName": cfg.source_table, **cfg.src_options}
    pks = cfg.primary_keys
    dest = cfg.destination_table

    def _merge_batch(batch_df: DataFrame, _batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        if spark.catalog.tableExists(dest):
            merge_cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in pks)
            (
                DeltaTable.forName(spark, dest)
                .alias("t")
                .merge(batch_df.alias("s"), merge_cond)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            # First run: create managed table
            batch_df.write.format("delta").mode("overwrite").saveAsTable(dest)

    (
        spark.readStream.format("lakeflow_connect")
        .options(**options)
        .load()
        .writeStream
        .foreachBatch(_merge_batch)
        .option("checkpointLocation", cfg.checkpoint_path)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )


def _ingest_snapshot(spark: SparkSession, api_token: str, cfg: _TableConfig) -> None:
    """
    Batch full-refresh → overwrite managed Delta table.
    Re-creates the table from scratch on every pipeline run.
    """
    options = {"api_token": api_token, "tableName": cfg.source_table, **cfg.src_options}
    (
        spark.read.format("lakeflow_connect")
        .options(**options)
        .load()
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(cfg.destination_table)
    )


def _ingest_append(spark: SparkSession, api_token: str, cfg: _TableConfig) -> None:
    """
    Streaming append-only → Delta managed table.
    """
    options = {"api_token": api_token, "tableName": cfg.source_table, **cfg.src_options}
    (
        spark.readStream.format("lakeflow_connect")
        .options(**options)
        .load()
        .writeStream
        .format("delta")
        .option("checkpointLocation", cfg.checkpoint_path)
        .trigger(availableNow=True)
        .toTable(cfg.destination_table)
        .awaitTermination()
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

# Keys that configure pipeline behaviour, not passed to the source connector.
_PIPELINE_KEYS = {"scd_type", "primary_keys", "sequence_by"}


def ingest_hms(spark: SparkSession, pipeline_spec: dict) -> None:
    """
    Ingest tables from a Lakeflow community connector into Hive Metastore
    (or any catalog) managed Delta tables.

    NOTE: registers the `lakeflow_connect` Spark data source on first call.

    Parameters
    ----------
    spark : SparkSession
    pipeline_spec : dict
        {
            "api_token": "<monday_token>",          # required
            "target_database": "monday_db",         # required — schema name
            "destination_catalog": None,            # optional — UC catalog (omit for HMS)
            "checkpoint_base":  "/dbfs/checkpoints/monday",      # DBFS path  (classic cluster)
            "checkpoint_volume": "/Volumes/cat/schema/vol/ckpt", # UC Volume   (serverless / UC)
            # checkpoint_volume takes precedence over checkpoint_base when both are set.
            "objects": [
                {
                    "table": {
                        "source_table": "boards",
                        # optional overrides:
                        "destination_catalog": None,   # e.g. "hive_metastore"
                        "destination_schema":  None,   # defaults to target_database
                        "destination_table":   None,   # defaults to source_table
                        "table_configuration": {
                            "state": "active",         # connector-specific option
                            "scd_type": "SCD_TYPE_1",  # pipeline option
                            "primary_keys": ["id"],    # pipeline option (override)
                        }
                    }
                },
                ...
            ]
        }

    Notes
    -----
    * Tables are created as managed Delta tables (no LOCATION clause), so they
      live inside the metastore's default warehouse directory.
    * To target Unity Catalog instead of HMS, set destination_catalog to your
      UC catalog name (e.g. "my_catalog"). The rest of the logic is identical.
    * checkpoint_volume — path inside a UC Volume (e.g. /Volumes/cat/schema/vol/ckpt).
      Use this on serverless compute where /dbfs is unavailable.
    * checkpoint_base   — DBFS path (e.g. /dbfs/checkpoints/monday).
      Use this on classic clusters.
    * checkpoint_volume takes precedence when both are supplied.
    * Delete a table's checkpoint dir to force a full re-sync of a CDC table.
    """
    from sources.monday._generated_monday_python_source import register_lakeflow_source
    register_lakeflow_source(spark)

    api_token: str = pipeline_spec.get("api_token", "")
    if not api_token:
        raise ValueError("'api_token' is required in pipeline_spec")

    target_database: str = pipeline_spec.get("target_database", "")
    if not target_database:
        raise ValueError("'target_database' is required in pipeline_spec")

    # Optional top-level catalog: when set, all tables default to catalog.target_database.table
    default_catalog: Optional[str] = pipeline_spec.get("destination_catalog")

    # checkpoint_volume (UC Volumes) takes precedence over checkpoint_base (DBFS)
    checkpoint_base: str = (
        pipeline_spec.get("checkpoint_volume")
        or pipeline_spec.get("checkpoint_base")
        or f"/dbfs/checkpoints/lakeflow/{target_database}"
    )

    objects: list = pipeline_spec.get("objects", [])
    if not objects:
        raise ValueError("'objects' must be a non-empty list in pipeline_spec")

    # Create schema — use 3-part name when catalog is specified (UC), 1-part for HMS
    if default_catalog:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{default_catalog}`.`{target_database}`")
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{target_database}`")

    # Build source option dicts (without pipeline-level keys)
    table_list = [o["table"]["source_table"] for o in objects]
    table_configs_for_metadata = {
        o["table"]["source_table"]: {
            k: v
            for k, v in (o["table"].get("table_configuration") or {}).items()
            if k not in _PIPELINE_KEYS
        }
        for o in objects
    }

    # Fetch connector-provided metadata
    metadata = _get_table_metadata(spark, api_token, table_list, table_configs_for_metadata)

    for obj in objects:
        tspec = obj["table"]
        source_table: str = tspec["source_table"]
        dest_table_name: str = tspec.get("destination_table") or source_table
        table_cfg: dict = tspec.get("table_configuration") or {}

        md = metadata.get(source_table, {})

        # primary_keys: spec wins over connector default
        primary_keys = table_cfg.get("primary_keys") or md.get("primary_keys") or ["id"]
        if isinstance(primary_keys, str):
            primary_keys = [k.strip() for k in primary_keys.split(",")]

        sequence_by: Optional[str] = table_cfg.get("sequence_by") or md.get("cursor_field")

        ingestion_type: str = md.get("ingestion_type", "cdc")
        scd_type_raw: str = table_cfg.get("scd_type", "SCD_TYPE_1").upper()
        if scd_type_raw == "APPEND_ONLY":
            ingestion_type = "append"
        scd_type = "2" if scd_type_raw == "SCD_TYPE_2" else "1"

        # Source options (connector-specific only)
        src_options = {k: v for k, v in table_cfg.items() if k not in _PIPELINE_KEYS}

        # Build fully-qualified destination table name
        # Per-table catalog overrides top-level default_catalog
        dest_catalog: Optional[str] = tspec.get("destination_catalog") or default_catalog
        dest_schema: str = tspec.get("destination_schema") or target_database
        if dest_catalog:
            full_dest = f"`{dest_catalog}`.`{dest_schema}`.`{dest_table_name}`"
        else:
            full_dest = f"`{dest_schema}`.`{dest_table_name}`"

        cfg = _TableConfig(
            source_table=source_table,
            destination_table=full_dest,
            src_options=src_options,
            primary_keys=primary_keys,
            sequence_by=sequence_by,
            scd_type=scd_type,
            ingestion_type=ingestion_type,
            checkpoint_path=f"{checkpoint_base}/{dest_table_name}",
        )

        print(f"[lakeflow-hms] {source_table} → {full_dest}  [{ingestion_type}]")

        if ingestion_type in ("cdc", "cdc_with_deletes"):
            _ingest_cdc(spark, api_token, cfg)
        elif ingestion_type == "snapshot":
            _ingest_snapshot(spark, api_token, cfg)
        elif ingestion_type == "append":
            _ingest_append(spark, api_token, cfg)
        else:
            # Unknown type — fall back to snapshot (safe default)
            print(f"  [warn] unknown ingestion_type '{ingestion_type}', falling back to snapshot")
            _ingest_snapshot(spark, api_token, cfg)

        print(f"  ✓ done")
