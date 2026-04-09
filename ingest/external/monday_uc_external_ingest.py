# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Monday.com → Unity Catalog (External Tables)
# MAGIC Ingests Monday.com data as **external** Delta tables in Unity Catalog.
# MAGIC Data is stored at `s3://webmotors-demo-ext-s3-049629455384-9pv221/monday/`.
# MAGIC
# MAGIC - External Location: `webmotors-demo-ext-role-049629455384-9pv221-el-01apcx`
# MAGIC - Schema: `webmotors_demo_catalog.monday_external`
# MAGIC - Serverless-compatible (no DBFS, no DataSource API)

# COMMAND ----------

import sys
repo_root = "/Repos/junior.esteca@databricks.com/lakeflow-monday-connector"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

from pipeline.ingestion_pipeline_uc_external_direct import ingest_uc_external_direct

# COMMAND ----------

dbutils.widgets.text("catalog",                "webmotors_demo_catalog",                                    "UC Catalog")
dbutils.widgets.text("schema",                 "monday_external",                                           "UC Schema")
dbutils.widgets.text("external_location_base", "s3://webmotors-demo-ext-s3-049629455384-9pv221/monday",     "External Location Base")
catalog                = dbutils.widgets.get("catalog")
schema                 = dbutils.widgets.get("schema")
external_location_base = dbutils.widgets.get("external_location_base")

# COMMAND ----------

pipeline_spec = {
    "api_token": dbutils.secrets.get(scope="monday", key="api_token"),

    # UC destination
    "destination_catalog": catalog,
    "destination_schema":  schema,

    # External Location base (registered in this workspace)
    "external_location_base": external_location_base,

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

ingest_uc_external_direct(spark, pipeline_spec)
