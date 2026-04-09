# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Monday.com → Unity Catalog (External — ForEach per Board)
# MAGIC
# MAGIC Executed once per board by the ForEach task in the job.
# MAGIC Receives a single `board_id` via the `{{input}}` parameter.
# MAGIC
# MAGIC - Schema: `classic_stable_hj897w_catalog.monday_external_foreach`
# MAGIC - Storage: `s3://classic-stable-hj897w-ext-s3-049629455384-xlik9z/monday_foreach/`
# MAGIC - Serverless-compatible

# COMMAND ----------

import sys
repo_root = "/Repos/erico.silva@databricks.com/lakeflow-monday-connector"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

from pipeline.ingestion_pipeline_uc_external_direct import ingest_uc_external_direct

# COMMAND ----------

dbutils.widgets.text("catalog",                "classic_stable_hj897w_catalog",                                           "UC Catalog")
dbutils.widgets.text("schema",                 "monday_external_foreach",                                          "UC Schema")
dbutils.widgets.text("external_location_base", "s3://classic-stable-hj897w-ext-s3-049629455384-xlik9z/monday_foreach",    "External Location Base")
catalog                = dbutils.widgets.get("catalog")
schema                 = dbutils.widgets.get("schema")
external_location_base = dbutils.widgets.get("external_location_base")

# COMMAND ----------

# board_id is injected by the ForEach task as the notebook widget "input"
dbutils.widgets.text("input", "", "Board ID")
board_id = dbutils.widgets.get("input").strip()

if not board_id:
    raise ValueError(
        "Widget 'input' is empty. This notebook must be run inside a ForEach task, "
        "or set the widget value manually for interactive testing."
    )

print(f"Processing board_id: {board_id}")

# COMMAND ----------

pipeline_spec = {
    "api_token": dbutils.secrets.get(scope="monday", key="api_token"),

    "destination_catalog": catalog,
    "destination_schema":  schema,

    "external_location_base": external_location_base,

    "objects": [
        {
            "table": {
                "source_table": "boards",
                "table_configuration": {
                    "state": "all",
                    "board_ids": board_id,
                },
            }
        },
        {
            "table": {
                "source_table": "items",
                "table_configuration": {"board_ids": board_id},
            }
        },
        {
            "table": {
                "source_table": "groups",
                "table_configuration": {"board_ids": board_id},
            }
        },
        {
            "table": {
                "source_table": "tags",
                "table_configuration": {"board_ids": board_id},
            }
        },
        {
            "table": {
                "source_table": "activity_logs",
                "table_configuration": {"board_ids": board_id},
            }
        },
        # account-level tables (users, workspaces, teams, updates) are intentionally
        # excluded here — they run in a dedicated downstream task to avoid
        # ConcurrentWriteException when multiple boards execute in parallel.
    ],
}

# COMMAND ----------

ingest_uc_external_direct(spark, pipeline_spec)
