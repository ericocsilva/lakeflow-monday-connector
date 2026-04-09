# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Monday.com → Unity Catalog (External — ForEach Account Tables)
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
# MAGIC - Schema: `webmotors_demo_catalog.monday_external_foreach`
# MAGIC - Storage: `s3://webmotors-demo-ext-s3-049629455384-9pv221/monday_foreach/`
# MAGIC - Serverless-compatible

# COMMAND ----------

import sys
repo_root = "/Repos/junior.esteca@databricks.com/lakeflow-monday-connector"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

from pipeline.ingestion_pipeline_uc_external_direct import ingest_uc_external_direct

# COMMAND ----------

dbutils.widgets.text("catalog",                "webmotors_demo_catalog",                                           "UC Catalog")
dbutils.widgets.text("schema",                 "monday_external_foreach",                                          "UC Schema")
dbutils.widgets.text("external_location_base", "s3://webmotors-demo-ext-s3-049629455384-9pv221/monday_foreach",    "External Location Base")
catalog                = dbutils.widgets.get("catalog")
schema                 = dbutils.widgets.get("schema")
external_location_base = dbutils.widgets.get("external_location_base")

# COMMAND ----------

pipeline_spec = {
    "api_token": dbutils.secrets.get(scope="monday", key="api_token"),

    "destination_catalog":    catalog,
    "destination_schema":     schema,
    "external_location_base": external_location_base,

    "objects": [
        {"table": {"source_table": "users"}},
        {"table": {"source_table": "workspaces"}},
        {"table": {"source_table": "teams"}},
        {"table": {"source_table": "updates"}},
    ],
}

# COMMAND ----------

ingest_uc_external_direct(spark, pipeline_spec)
