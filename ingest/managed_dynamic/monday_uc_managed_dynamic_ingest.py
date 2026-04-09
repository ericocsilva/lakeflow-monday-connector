# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Monday.com → Unity Catalog (Managed Tables — Dynamic Board IDs)
# MAGIC
# MAGIC Task 2 of the dynamic ingestion pipeline.
# MAGIC
# MAGIC - Receives `board_ids` from the upstream `discover_boards` task via Task Values
# MAGIC - Ingests all boards and their items into managed Delta tables in Unity Catalog
# MAGIC - Schema: `webmotors_demo_catalog.monday_dynamic`

# COMMAND ----------

import sys
repo_root = "/Repos/junior.esteca@databricks.com/lakeflow-monday-connector"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

from pipeline.ingestion_pipeline_direct import ingest_direct

# COMMAND ----------

dbutils.widgets.text("catalog", "webmotors_demo_catalog", "UC Catalog")
dbutils.widgets.text("schema",  "monday_dynamic",         "UC Schema")
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------

# Read board_ids published by the upstream discover_boards task
board_ids = dbutils.jobs.taskValues.get(
    taskKey="discover_boards",
    key="board_ids",
    default="",         # fallback for manual/ad-hoc runs
    debugValue="",      # value used when running the notebook interactively
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

    "target_database":     schema,
    "destination_catalog": catalog,

    "objects": [
        # boards — CDC, no board_ids filter (fetches all boards)
        {
            "table": {
                "source_table": "boards",
                "table_configuration": {"state": "active"},
            }
        },
        # items — CDC, scoped to discovered board_ids
        {
            "table": {
                "source_table": "items",
                "table_configuration": {"board_ids": board_ids},
            }
        },
        # snapshot tables — scoped to discovered board_ids where applicable
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

ingest_direct(spark, pipeline_spec)
