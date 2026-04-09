"""
UC External Table ingestion pipeline for Lakeflow Community Connectors.

Alternative to ingestion_pipeline_hms.py that writes EXTERNAL Delta tables
registered in Unity Catalog.

Key differences from the HMS version:
- Tables are external (data stored at a user-supplied cloud path)
- The cloud path must be under a UC External Location (ABFSS, GCS, S3, etc.)
- Table registration done in UC via saveAsTable() with an explicit LOCATION
- destination_catalog is required (UC catalog)
- Checkpoints default to a _checkpoints/ prefix inside the external_location_base,
  but can be overridden with checkpoint_base

Ingestion behaviour per table type:
  cdc      → readStream + foreachBatch + MERGE  (boards, items)
  snapshot → batch read  + overwrite            (users, workspaces, teams, …)
  append   → readStream  + append               (forced via scd_type=APPEND_ONLY)

External table location layout:
  <external_location_base>/
    <dest_table_name>/          ← Delta table data
    _checkpoints/
      <dest_table_name>/        ← Spark Streaming checkpoint (CDC/append only)
"""

import json
from dataclasses import dataclass
from typing import Dict, List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


# ---------------------------------------------------------------------------
# Internal config dataclass
# ---------------------------------------------------------------------------

@dataclass
class _UcExternalTableConfig:
    source_table: str
    destination_table: str   # `catalog`.`schema`.`table`
    location: str            # cloud path for table data
    checkpoint_path: str     # cloud/DBFS path for streaming checkpoint
    src_options: Dict[str, str]
    primary_keys: List[str]
    sequence_by: Optional[str]
    scd_type: str            # "1" or "2"
    ingestion_type: str      # "cdc" | "snapshot" | "append"


# ---------------------------------------------------------------------------
# Metadata helper (identical to HMS version — no UC dependency)
# ---------------------------------------------------------------------------

def _get_table_metadata(
    spark: SparkSession,
    api_token: str,
    table_list: List[str],
    table_configs: Dict[str, Dict],
) -> Dict[str, Dict]:
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
# UC external table creation helpers
# ---------------------------------------------------------------------------

def _register_external_table_if_needed(
    spark: SparkSession,
    full_table_name: str,
    schema_ddl: str,
    location: str,
) -> None:
    """
    Register an empty external Delta table in UC if it doesn't exist yet.
    This approach separates table registration from data writing, which
    avoids permissions issues on the first batch.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name}
        ({schema_ddl})
        USING DELTA
        LOCATION '{location}'
    """)


def _first_write_external(
    spark: SparkSession,
    df: DataFrame,
    full_table_name: str,
    location: str,
) -> None:
    """
    Write first batch as an external Delta table at `location` and register
    it in UC under `full_table_name`.

    saveAsTable() with path= creates an EXTERNAL table in UC when the path
    is under a registered External Location.
    """
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("path", location)
        .saveAsTable(full_table_name)
    )


# ---------------------------------------------------------------------------
# Ingestion strategies
# ---------------------------------------------------------------------------

def _ingest_cdc(spark: SparkSession, api_token: str, cfg: _UcExternalTableConfig) -> None:
    """
    Streaming CDC → external Delta table in UC via MERGE.

    First micro-batch: creates the external table at cfg.location and
    registers it in UC under cfg.destination_table.
    Subsequent micro-batches: MERGE by primary keys.
    """
    options = {"api_token": api_token, "tableName": cfg.source_table, **cfg.src_options}
    pks = cfg.primary_keys
    dest = cfg.destination_table
    location = cfg.location

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
            _first_write_external(spark, batch_df, dest, location)

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


def _ingest_snapshot(spark: SparkSession, api_token: str, cfg: _UcExternalTableConfig) -> None:
    """
    Full-refresh snapshot → external Delta table in UC.

    Uses overwriteSchema=true so schema evolution doesn't break re-runs.
    The external table is re-registered on first run; subsequent runs just
    overwrite the data at the same location.
    """
    options = {"api_token": api_token, "tableName": cfg.source_table, **cfg.src_options}
    (
        spark.read.format("lakeflow_connect")
        .options(**options)
        .load()
        .write
        .format("delta")
        .mode("overwrite")
        .option("path", cfg.location)
        .option("overwriteSchema", "true")
        .saveAsTable(cfg.destination_table)
    )


def _ingest_append(spark: SparkSession, api_token: str, cfg: _UcExternalTableConfig) -> None:
    """
    Streaming append-only → external Delta table in UC.
    Creates the table on first run, then keeps appending.
    """
    options = {"api_token": api_token, "tableName": cfg.source_table, **cfg.src_options}

    dest = cfg.destination_table
    location = cfg.location

    def _append_batch(batch_df: DataFrame, _batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        if spark.catalog.tableExists(dest):
            batch_df.write.format("delta").mode("append").option("path", location).save()
        else:
            _first_write_external(spark, batch_df, dest, location)

    (
        spark.readStream.format("lakeflow_connect")
        .options(**options)
        .load()
        .writeStream
        .foreachBatch(_append_batch)
        .option("checkpointLocation", cfg.checkpoint_path)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

_PIPELINE_KEYS = {"scd_type", "primary_keys", "sequence_by"}


def ingest_uc_external(spark: SparkSession, pipeline_spec: dict) -> None:
    """
    Ingest tables from a Lakeflow community connector into Unity Catalog
    as EXTERNAL Delta tables stored at a user-supplied cloud location.

    Parameters
    ----------
    spark : SparkSession
    pipeline_spec : dict
        {
            "api_token": "<monday_token>",          # required
            "destination_catalog": "my_catalog",    # required — UC catalog
            "destination_schema": "monday",         # required — UC schema
            "external_location_base": "abfss://container@account.dfs.core.windows.net/monday",
                                                    # required — must be under a UC External Location
            "checkpoint_base": None,                # optional — defaults to
                                                    #   <external_location_base>/_checkpoints
            "objects": [
                {
                    "table": {
                        "source_table": "boards",
                        # optional overrides:
                        "destination_table": "boards",   # defaults to source_table
                        "table_configuration": {
                            "state": "active",           # connector-specific option
                            "scd_type": "SCD_TYPE_1",    # SCD_TYPE_1 | SCD_TYPE_2 | APPEND_ONLY
                            "primary_keys": ["id"],      # override connector default
                        }
                    }
                },
                ...
            ]
        }

    Unity Catalog requirements
    --------------------------
    1. destination_catalog must already exist (CREATE CATALOG is out of scope here).
    2. The schema is created automatically if it doesn't exist.
    3. external_location_base must be under a registered UC External Location.
       Run in Databricks SQL before use:
         CREATE EXTERNAL LOCATION monday_location
           URL 'abfss://container@account.dfs.core.windows.net/monday'
           WITH (STORAGE CREDENTIAL my_credential);
    4. The cluster's service principal must have CREATE TABLE, WRITE FILES
       privileges on the schema and the external location.

    Table location layout
    ---------------------
    <external_location_base>/<dest_table_name>/
    <external_location_base>/_checkpoints/<dest_table_name>/
    """
    from sources.monday._generated_monday_python_source import register_lakeflow_source
    register_lakeflow_source(spark)

    api_token: str = pipeline_spec.get("api_token", "")
    if not api_token:
        raise ValueError("'api_token' is required in pipeline_spec")

    dest_catalog: str = pipeline_spec.get("destination_catalog", "")
    if not dest_catalog:
        raise ValueError("'destination_catalog' is required in pipeline_spec")

    dest_schema: str = pipeline_spec.get("destination_schema", "")
    if not dest_schema:
        raise ValueError("'destination_schema' is required in pipeline_spec")

    ext_base: str = pipeline_spec.get("external_location_base", "").rstrip("/")
    if not ext_base:
        raise ValueError("'external_location_base' is required in pipeline_spec")

    checkpoint_base: str = (
        pipeline_spec.get("checkpoint_base") or f"{ext_base}/_checkpoints"
    ).rstrip("/")

    objects: list = pipeline_spec.get("objects", [])
    if not objects:
        raise ValueError("'objects' must be a non-empty list in pipeline_spec")

    # Ensure target schema exists in UC
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS `{dest_catalog}`.`{dest_schema}`"
    )

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

        # Per-table overrides for catalog/schema
        effective_catalog = tspec.get("destination_catalog") or dest_catalog
        effective_schema = tspec.get("destination_schema") or dest_schema
        full_dest = f"`{effective_catalog}`.`{effective_schema}`.`{dest_table_name}`"

        # External location for this table's data
        location = f"{ext_base}/{dest_table_name}"
        checkpoint_path = f"{checkpoint_base}/{dest_table_name}"

        cfg = _UcExternalTableConfig(
            source_table=source_table,
            destination_table=full_dest,
            location=location,
            checkpoint_path=checkpoint_path,
            src_options=src_options,
            primary_keys=primary_keys,
            sequence_by=sequence_by,
            scd_type=scd_type,
            ingestion_type=ingestion_type,
        )

        print(f"[lakeflow-uc-external] {source_table} → {full_dest}")
        print(f"  location:   {location}")
        print(f"  type:       {ingestion_type}")

        if ingestion_type in ("cdc", "cdc_with_deletes"):
            _ingest_cdc(spark, api_token, cfg)
        elif ingestion_type == "snapshot":
            _ingest_snapshot(spark, api_token, cfg)
        elif ingestion_type == "append":
            _ingest_append(spark, api_token, cfg)
        else:
            print(f"  [warn] unknown ingestion_type '{ingestion_type}', falling back to snapshot")
            _ingest_snapshot(spark, api_token, cfg)

        print(f"  done")
