# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Monday.com → Unity Catalog (Managed Tables — Classic Compute)
# MAGIC Ingests Monday.com data as managed Delta tables in Unity Catalog.
# MAGIC Uses `ingestion_pipeline_hms` — requires classic compute (DBR 14.3+).
# MAGIC Uses Spark Structured Streaming + foreachBatch for CDC tables.

# COMMAND ----------

import sys
repo_root = "/Repos/junior.esteca@databricks.com/lakeflow-monday-connector"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

from pipeline.ingestion_pipeline_hms import ingest_hms

# COMMAND ----------

dbutils.widgets.text("catalog",         "classic_stable_hj897w_catalog",           "UC Catalog")
dbutils.widgets.text("schema",          "monday",                            "UC Schema")
dbutils.widgets.text("checkpoint_base", "/dbfs/checkpoints/lakeflow/monday", "Checkpoint Base (DBFS)")
catalog         = dbutils.widgets.get("catalog")
schema          = dbutils.widgets.get("schema")
checkpoint_base = dbutils.widgets.get("checkpoint_base")

# COMMAND ----------

pipeline_spec = {
    "api_token": dbutils.secrets.get(scope="monday", key="api_token"),

    "target_database":     schema,
    "destination_catalog": catalog,
    "checkpoint_base":     checkpoint_base,

    "objects": [
        {
            "table": {
                "source_table": "boards",
                "table_configuration": {"state": "all"},
            }
        },
        {
            "table": {
                "source_table": "items",
                "table_configuration": {
                    "board_ids": "18406865721",  # Poc Monday board
                },
            }
        },
        {"table": {"source_table": "users"}},
        {"table": {"source_table": "workspaces"}},
        {"table": {"source_table": "teams"}},
        {
            "table": {
                "source_table": "groups",
                "table_configuration": {"board_ids": "18406865721"},
            }
        },
        {
            "table": {
                "source_table": "tags",
                "table_configuration": {"board_ids": "18406865721"},
            }
        },
        {"table": {"source_table": "updates"}},
        {
            "table": {
                "source_table": "activity_logs",
                "table_configuration": {"board_ids": "18406865721"},
            }
        },
    ],
}

# COMMAND ----------

ingest_hms(spark, pipeline_spec)
