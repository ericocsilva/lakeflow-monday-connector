# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Monday.com → Unity Catalog (External Tables — Dynamic Board IDs — Classic Compute)
# MAGIC
# MAGIC Task 2 of the dynamic ingestion pipeline.
# MAGIC
# MAGIC - Receives `board_ids` from the upstream `discover_boards` task via Task Values
# MAGIC - Uses `ingestion_pipeline_uc_external` — requires classic compute (DBR 14.3+)
# MAGIC - Uses Spark Structured Streaming + foreachBatch for CDC tables

# COMMAND ----------

import sys
repo_root = "/Repos/erico.silva@databricks.com/lakeflow-monday-connector"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

from pipeline.ingestion_pipeline_uc_external import ingest_uc_external

# COMMAND ----------

dbutils.widgets.text("catalog",                "classic_stable_hj897w_catalog",                                           "UC Catalog")
dbutils.widgets.text("schema",                 "monday_external_dynamic",                                          "UC Schema")
dbutils.widgets.text("external_location_base", "s3://classic-stable-hj897w-ext-s3-049629455384-xlik9z/monday_dynamic",    "External Location Base")
dbutils.widgets.text("checkpoint_base",        "/dbfs/checkpoints/lakeflow/monday_external_dynamic",               "Checkpoint Base (DBFS)")
catalog                = dbutils.widgets.get("catalog")
schema                 = dbutils.widgets.get("schema")
external_location_base = dbutils.widgets.get("external_location_base")
checkpoint_base        = dbutils.widgets.get("checkpoint_base")

# COMMAND ----------

# Read board_ids published by the upstream discover_boards task
board_ids = dbutils.jobs.taskValues.get(
    taskKey="discover_boards",
    key="board_ids",
    default="",
    debugValue="",
)

if not board_ids:
    raise ValueError(
        "No board_ids received from discover_boards task. "
        "Run this notebook as part of the full job, or set debugValue to a board ID list."
    )

print(f"Received board_ids: {board_ids}")

# COMMAND ----------

pipeline_spec = {
    "api_token": dbutils.secrets.get(scope="monday", key="api_token"),

    "destination_catalog":    catalog,
    "destination_schema":     schema,
    "external_location_base": external_location_base,
    "checkpoint_base":        checkpoint_base,

    "objects": [
        {
            "table": {
                "source_table": "boards",
                "table_configuration": {"state": "active"},
            }
        },
        {
            "table": {
                "source_table": "items",
                "table_configuration": {"board_ids": board_ids},
            }
        },
        {"table": {"source_table": "users"}},
        {"table": {"source_table": "workspaces"}},
        {"table": {"source_table": "teams"}},
        {
            "table": {
                "source_table": "groups",
                "table_configuration": {"board_ids": board_ids},
            }
        },
        {
            "table": {
                "source_table": "tags",
                "table_configuration": {"board_ids": board_ids},
            }
        },
        {"table": {"source_table": "updates"}},
        {
            "table": {
                "source_table": "activity_logs",
                "table_configuration": {"board_ids": board_ids},
            }
        },
    ],
}

# COMMAND ----------

ingest_uc_external(spark, pipeline_spec)
