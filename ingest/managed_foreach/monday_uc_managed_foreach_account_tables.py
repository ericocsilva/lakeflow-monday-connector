# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Monday.com → Unity Catalog (Managed — ForEach Account Tables)
# MAGIC
# MAGIC Task 3 of the ForEach ingestion pipeline. Runs **once** after all per-board
# MAGIC iterations complete.
# MAGIC
# MAGIC Ingests account-level tables that are not scoped to a specific board:
# MAGIC `users`, `workspaces`, `teams`, `updates`.
# MAGIC
# MAGIC These tables are intentionally excluded from the ForEach iteration notebook
# MAGIC to avoid `ConcurrentWriteException` when multiple boards run in parallel.
# MAGIC
# MAGIC - Schema: `classic_stable_hj897w_catalog.monday_foreach`
# MAGIC - Serverless-compatible

# COMMAND ----------

import sys
repo_root = "/Repos/erico.silva@databricks.com/lakeflow-monday-connector"
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

pipeline_spec = {
    "api_token": dbutils.secrets.get(scope="monday", key="api_token"),

    "target_database":     schema,
    "destination_catalog": catalog,

    "objects": [
        {"table": {"source_table": "users"}},
        {"table": {"source_table": "workspaces"}},
        {"table": {"source_table": "teams"}},
        {"table": {"source_table": "updates"}},
    ],
}

# COMMAND ----------

ingest_direct(spark, pipeline_spec)
