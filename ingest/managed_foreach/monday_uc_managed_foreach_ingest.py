# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Monday.com → Unity Catalog (Managed — ForEach per Board)
# MAGIC
# MAGIC Executed once per board by the ForEach task in the job.
# MAGIC Receives a single `board_id` via the `{{input}}` parameter.
# MAGIC
# MAGIC - Schema: `classic_stable_hj897w_catalog.monday_foreach`
# MAGIC - Serverless-compatible

# COMMAND ----------

import sys
repo_root = "/Repos/junior.esteca@databricks.com/lakeflow-monday-connector"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

from pipeline.ingestion_pipeline_direct import ingest_direct

# COMMAND ----------

dbutils.widgets.text("catalog", "classic_stable_hj897w_catalog", "UC Catalog")
dbutils.widgets.text("schema",  "monday_foreach",         "UC Schema")
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------

# board_id is injected by the ForEach task as the notebook widget "input"
# When running interactively, set a value in the widget or pass via dbutils.widgets
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

    "target_database":     schema,
    "destination_catalog": catalog,

    "objects": [
        # boards — scoped to this specific board
        {
            "table": {
                "source_table": "boards",
                "table_configuration": {
                    "state": "all",
                    "board_ids": board_id,
                },
            }
        },
        # items — scoped to this board
        {
            "table": {
                "source_table": "items",
                "table_configuration": {"board_ids": board_id},
            }
        },
        # board-level snapshot tables
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

ingest_direct(spark, pipeline_spec)
