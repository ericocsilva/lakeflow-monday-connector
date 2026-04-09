# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Monday.com → Unity Catalog (External — ForEach Account Tables — Classic Compute)
# MAGIC
# MAGIC Task 3 of the ForEach ingestion pipeline. Runs **once** after all per-board
# MAGIC iterations complete.
# MAGIC
# MAGIC Ingests account-level tables that are not scoped to a specific board:
# MAGIC `users`, `workspaces`, `teams`, `updates`.
# MAGIC
# MAGIC Uses `ingestion_pipeline_uc_external` — requires classic compute (DBR 14.3+).

# COMMAND ----------

import sys
repo_root = "/Repos/erico.silva@databricks.com/lakeflow-monday-connector"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

from pipeline.ingestion_pipeline_uc_external import ingest_uc_external

# COMMAND ----------

dbutils.widgets.text("catalog",                "classic_stable_hj897w_catalog",                                           "UC Catalog")
dbutils.widgets.text("schema",                 "monday_external_foreach",                                          "UC Schema")
dbutils.widgets.text("external_location_base", "s3://classic-stable-hj897w-ext-s3-049629455384-xlik9z/monday_foreach",    "External Location Base")
dbutils.widgets.text("checkpoint_base",        "/dbfs/checkpoints/lakeflow/monday_external_foreach",               "Checkpoint Base (DBFS)")
catalog                = dbutils.widgets.get("catalog")
schema                 = dbutils.widgets.get("schema")
external_location_base = dbutils.widgets.get("external_location_base")
checkpoint_base        = dbutils.widgets.get("checkpoint_base")

# COMMAND ----------

pipeline_spec = {
    "api_token": dbutils.secrets.get(scope="monday", key="api_token"),

    "destination_catalog":    catalog,
    "destination_schema":     schema,
    "external_location_base": external_location_base,
    "checkpoint_base":        checkpoint_base,

    "objects": [
        {"table": {"source_table": "users"}},
        {"table": {"source_table": "workspaces"}},
        {"table": {"source_table": "teams"}},
        {"table": {"source_table": "updates"}},
    ],
}

# COMMAND ----------

ingest_uc_external(spark, pipeline_spec)
