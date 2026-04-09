# Databricks notebook — Monday.com → Hive Metastore
# =====================================================================
# Alternative to pipeline-spec/example_ingest.py that works WITHOUT
# Unity Catalog. Tables are created as managed Delta tables in HMS.
#
# Prerequisites
# -------------
# 1. Attach this notebook (and the repo files) to a cluster that has
#    the `requests` library available (standard in DBR 11+).
# 2. Place this notebook and the supporting directories
#    (pipeline/, libs/, sources/monday/) in the same Databricks Repo
#    or upload them as a Workspace folder.
#
# File layout expected
# --------------------
#   pipeline/
#     ingestion_pipeline_hms.py   ← HMS pipeline (this project)
#     ingestion_pipeline.py       ← original SDP pipeline (unchanged)
#   libs/
#     source_loader.py            ← from GitHub repo (unchanged)
#     spec_parser.py              ← from GitHub repo (unchanged)
#     utils.py                    ← from GitHub repo (unchanged)
#   sources/monday/
#     __init__.py                 ← empty file (needed for import)
#     _generated_monday_python_source.py  ← from GitHub repo (unchanged)
# =====================================================================

# COMMAND ----------

# If running outside a DBR cluster that already has `requests`:
# %pip install requests

# COMMAND ----------

import sys, os
# Add repo root to path when running as a script (not needed inside Databricks Repos)
repo_root = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else "/Workspace/Repos/<your-repo>"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

from pipeline.ingestion_pipeline_hms import ingest_hms
from libs.source_loader import get_register_function

# Register the Monday.com DataSource with this Spark session
register_lakeflow_source = get_register_function("monday")
register_lakeflow_source(spark)  # `spark` is injected by the Databricks notebook context

# COMMAND ----------

# =============================================================================
# PIPELINE CONFIGURATION
# =============================================================================
#
# api_token      : Monday.com personal API token.
#                  Best practice: use dbutils.secrets instead of a plain string.
#                    dbutils.secrets.get(scope="monday", key="api_token")
#
# target_database: Hive Metastore database where tables will be created.
#                  The database is created automatically if it doesn't exist.
#
# checkpoint_base: DBFS path for Spark Streaming checkpoints.
#                  Used only for CDC tables (boards, items).
#                  Delete a table's checkpoint dir to force a full re-sync.
#
# destination_catalog (optional):
#                  Set to a Unity Catalog catalog name (e.g. "my_catalog") to
#                  write to UC instead of HMS — the pipeline logic is identical.
#
# table_configuration keys:
#   board_ids   — comma-separated Monday board IDs (items, groups, tags, activity_logs)
#   page_size   — records per API page (boards, items, users)
#   state       — board/workspace filter: active | all | archived | deleted
#   kind        — user filter: all | guests | non_guests | non_pending
#   scd_type    — SCD_TYPE_1 (default) | SCD_TYPE_2 | APPEND_ONLY
#   primary_keys— override connector-default primary keys (comma-separated or list)
# =============================================================================

pipeline_spec = {
    # ---- Authentication -------------------------------------------------------
    "api_token": dbutils.secrets.get(scope="monday", key="api_token"),
    # "api_token": "<paste-token-here-for-quick-testing>",

    # ---- Destination ----------------------------------------------------------
    "target_database": "monday_db",               # HMS database name
    "checkpoint_base": "/dbfs/checkpoints/monday", # DBFS checkpoint root

    # ---- Tables to ingest -----------------------------------------------------
    "objects": [
        # CDC tables — incremental on subsequent runs via activity log cursor
        {
            "table": {
                "source_table": "boards",
                "table_configuration": {
                    "state": "active",             # only active boards
                },
            }
        },
        {
            "table": {
                "source_table": "items",
                "table_configuration": {
                    # "board_ids": "18394670765,18394670764",  # optional: restrict boards
                },
            }
        },
        # Snapshot tables — full refresh on every run
        {"table": {"source_table": "users"}},
        {"table": {"source_table": "workspaces"}},
        {"table": {"source_table": "teams"}},
        {"table": {"source_table": "groups"}},
        {"table": {"source_table": "tags"}},
        {"table": {"source_table": "updates"}},
        {"table": {"source_table": "activity_logs"}},

        # Example: write a table to a specific schema instead of target_database
        # {
        #     "table": {
        #         "source_table": "users",
        #         "destination_schema": "monday_raw",
        #         "destination_table": "monday_users",
        #     }
        # },

        # Example: write to Unity Catalog instead of HMS
        # {
        #     "table": {
        #         "source_table": "boards",
        #         "destination_catalog": "my_uc_catalog",
        #         "destination_schema":  "monday",
        #         "destination_table":   "boards",
        #     }
        # },
    ],
}

# COMMAND ----------

ingest_hms(spark, pipeline_spec)
