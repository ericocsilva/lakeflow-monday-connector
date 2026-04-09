# ---------------------------------------------------------------------------
# Job 1 — Static · Managed · Classic
# 1 task: ingests all tables into a single managed schema
# ---------------------------------------------------------------------------

resource "databricks_job" "monday_managed_classic" {
  name = "monday-lakeflow-managed-classic"

  task {
    task_key            = "ingest_monday_managed"
    existing_cluster_id = var.cluster_id

    notebook_task {
      notebook_path = "${var.repo_path}/ingest/managed/monday_uc_managed_classic_ingest"
      source        = "WORKSPACE"

      base_parameters = {
        catalog         = var.catalog
        schema          = var.managed_schema
        checkpoint_base = "${var.checkpoint_base}/monday"
      }
    }
  }

  schedule {
    quartz_cron_expression = var.schedule_cron
    timezone_id            = var.schedule_timezone
    pause_status           = var.schedule_pause_status
  }
}

# ---------------------------------------------------------------------------
# Job 2 — Static · External · Classic
# 1 task: ingests all tables as external Delta at external_location_base
# ---------------------------------------------------------------------------

resource "databricks_job" "monday_external_classic" {
  name = "monday-lakeflow-external-classic"

  task {
    task_key            = "ingest_monday_external"
    existing_cluster_id = var.cluster_id

    notebook_task {
      notebook_path = "${var.repo_path}/ingest/external/monday_uc_external_classic_ingest"
      source        = "WORKSPACE"

      base_parameters = {
        catalog                = var.catalog
        schema                 = var.external_schema
        external_location_base = var.external_location_base
        checkpoint_base        = "${var.checkpoint_base}/monday_external"
      }
    }
  }

  schedule {
    quartz_cron_expression = var.schedule_cron
    timezone_id            = var.schedule_timezone
    pause_status           = var.schedule_pause_status
  }
}

# ---------------------------------------------------------------------------
# Job 3 — Dynamic · Managed · Classic
# Task 1: discover_boards — queries Monday.com API and publishes board_ids
# Task 2: ingest         — reads board_ids via Task Values, ingests managed tables
# ---------------------------------------------------------------------------

resource "databricks_job" "monday_managed_dynamic_classic" {
  name = "monday-lakeflow-managed-dynamic-classic"

  task {
    task_key            = "discover_boards"
    existing_cluster_id = var.cluster_id

    notebook_task {
      notebook_path = "${var.repo_path}/ingest/discover_boards"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key            = "ingest_monday_managed_dynamic"
    existing_cluster_id = var.cluster_id

    depends_on {
      task_key = "discover_boards"
    }

    notebook_task {
      notebook_path = "${var.repo_path}/ingest/managed_dynamic/monday_uc_managed_dynamic_classic_ingest"
      source        = "WORKSPACE"

      base_parameters = {
        catalog         = var.catalog
        schema          = var.managed_dynamic_schema
        checkpoint_base = "${var.checkpoint_base}/monday_dynamic"
      }
    }
  }

  schedule {
    quartz_cron_expression = var.schedule_cron
    timezone_id            = var.schedule_timezone
    pause_status           = var.schedule_pause_status
  }
}

# ---------------------------------------------------------------------------
# Job 5 — ForEach · External · Classic
# Task 1: discover_boards       — queries Monday.com API and publishes board_ids
# Task 2: foreach_ingest        — one iteration per board_id (concurrency=1)
# Task 3: ingest_account_tables — account-wide tables after ForEach
# ---------------------------------------------------------------------------

resource "databricks_job" "monday_external_foreach_classic" {
  name = "monday-lakeflow-external-foreach-classic"

  task {
    task_key            = "discover_boards"
    existing_cluster_id = var.cluster_id

    notebook_task {
      notebook_path = "${var.repo_path}/ingest/discover_boards"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "foreach_ingest"

    depends_on {
      task_key = "discover_boards"
    }

    for_each_task {
      inputs      = "{{tasks.discover_boards.values.board_ids_json}}"
      concurrency = 1

      task {
        task_key            = "foreach_ingest_iteration"
        existing_cluster_id = var.cluster_id

        notebook_task {
          notebook_path = "${var.repo_path}/ingest/external_foreach/monday_uc_external_foreach_classic_ingest"
          source        = "WORKSPACE"

          base_parameters = {
            catalog                = var.catalog
            schema                 = var.external_foreach_schema
            external_location_base = var.external_foreach_location_base
            checkpoint_base        = "${var.checkpoint_base}/monday_external_foreach"
            input                  = "{{input}}"
          }
        }
      }
    }
  }

  task {
    task_key            = "ingest_account_tables"
    existing_cluster_id = var.cluster_id

    depends_on {
      task_key = "foreach_ingest"
    }

    notebook_task {
      notebook_path = "${var.repo_path}/ingest/external_foreach/monday_uc_external_foreach_account_tables_classic"
      source        = "WORKSPACE"

      base_parameters = {
        catalog                = var.catalog
        schema                 = var.external_foreach_schema
        external_location_base = var.external_foreach_location_base
      }
    }
  }

  schedule {
    quartz_cron_expression = var.schedule_cron
    timezone_id            = var.schedule_timezone
    pause_status           = var.schedule_pause_status
  }
}

# ---------------------------------------------------------------------------
# Job 4 — Dynamic · External · Classic
# Task 1: discover_boards — queries Monday.com API and publishes board_ids
# Task 2: ingest         — reads board_ids via Task Values, ingests external tables
# ---------------------------------------------------------------------------

resource "databricks_job" "monday_external_dynamic_classic" {
  name = "monday-lakeflow-external-dynamic-classic"

  task {
    task_key            = "discover_boards"
    existing_cluster_id = var.cluster_id

    notebook_task {
      notebook_path = "${var.repo_path}/ingest/discover_boards"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key            = "ingest_monday_external_dynamic"
    existing_cluster_id = var.cluster_id

    depends_on {
      task_key = "discover_boards"
    }

    notebook_task {
      notebook_path = "${var.repo_path}/ingest/external_dynamic/monday_uc_external_dynamic_classic_ingest"
      source        = "WORKSPACE"

      base_parameters = {
        catalog                = var.catalog
        schema                 = var.external_dynamic_schema
        external_location_base = var.external_dynamic_location_base
        checkpoint_base        = "${var.checkpoint_base}/monday_external_dynamic"
      }
    }
  }

  schedule {
    quartz_cron_expression = var.schedule_cron
    timezone_id            = var.schedule_timezone
    pause_status           = var.schedule_pause_status
  }
}
