# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Monday.com → Unity Catalog (Managed Tables)
# MAGIC Ingests Monday.com data as managed Delta tables in Unity Catalog.
# MAGIC Uses `ingestion_pipeline_direct` — compatible with serverless and DBR 12.x+.

# COMMAND ----------

import sys
repo_root = "/Repos/junior.esteca@databricks.com/lakeflow-monday-connector"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

from pipeline.ingestion_pipeline_direct import ingest_direct

# COMMAND ----------

dbutils.widgets.text("catalog", "webmotors_demo_catalog", "UC Catalog")
dbutils.widgets.text("schema", "monday", "UC Schema")
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------

pipeline_spec = {
    "api_token": dbutils.secrets.get(scope="monday", key="api_token"),

    # Destination: Unity Catalog managed tables
    "target_database":     schema,
    "destination_catalog": catalog,

    # CDC offsets stored in <catalog>.<schema>._lakeflow_offsets (no DBFS needed)

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

ingest_direct(spark, pipeline_spec)
