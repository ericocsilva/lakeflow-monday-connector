# ---------------------------------------------------------------------------
# Workspace
# ---------------------------------------------------------------------------

variable "databricks_host" {
  description = "Databricks workspace URL (e.g. https://adb-1234567890.1.azuredatabricks.net)"
  type        = string
}

variable "databricks_token" {
  description = "Databricks personal access token. Prefer passing via TF_VAR_databricks_token env var."
  type        = string
  sensitive   = true
}

# ---------------------------------------------------------------------------
# Compute
# ---------------------------------------------------------------------------

variable "cluster_id" {
  description = "Existing classic cluster ID — must run DBR 14.3+."
  type        = string
}

# ---------------------------------------------------------------------------
# Repo
# ---------------------------------------------------------------------------

variable "repo_path" {
  description = "Absolute Workspace path to the lakeflow-monday-connector repo (e.g. /Repos/user@company.com/lakeflow-monday-connector)."
  type        = string
}

# ---------------------------------------------------------------------------
# Unity Catalog — destination
# ---------------------------------------------------------------------------

variable "catalog" {
  description = "UC catalog where schemas and tables will be created."
  type        = string
}

variable "managed_schema" {
  description = "Schema for static managed tables."
  type        = string
  default     = "monday"
}

variable "managed_dynamic_schema" {
  description = "Schema for dynamic managed tables."
  type        = string
  default     = "monday_dynamic"
}

variable "external_schema" {
  description = "Schema for static external tables."
  type        = string
  default     = "monday_external"
}

variable "external_dynamic_schema" {
  description = "Schema for dynamic external tables."
  type        = string
  default     = "monday_external_dynamic"
}

# ---------------------------------------------------------------------------
# External tables — storage
# ---------------------------------------------------------------------------

variable "external_location_base" {
  description = "Cloud storage base path for static external tables (e.g. s3://bucket/monday). Must be under a registered UC External Location."
  type        = string
}

variable "external_dynamic_location_base" {
  description = "Cloud storage base path for dynamic external tables (e.g. s3://bucket/monday_dynamic). Must be under a registered UC External Location."
  type        = string
}

variable "external_foreach_schema" {
  description = "Schema for ForEach external tables."
  type        = string
  default     = "monday_external_foreach"
}

variable "external_foreach_location_base" {
  description = "Cloud storage base path for ForEach external tables (e.g. s3://bucket/monday_foreach). Must be under a registered UC External Location."
  type        = string
}

# ---------------------------------------------------------------------------
# Checkpoints (classic / Structured Streaming)
# ---------------------------------------------------------------------------

variable "checkpoint_base" {
  description = "DBFS base path for Structured Streaming checkpoints."
  type        = string
  default     = "/dbfs/checkpoints/lakeflow"
}

# ---------------------------------------------------------------------------
# Schedule
# ---------------------------------------------------------------------------

variable "schedule_cron" {
  description = "Quartz cron expression for all jobs."
  type        = string
  default     = "0 0 * * * ?"
}

variable "schedule_timezone" {
  description = "Timezone for the job schedule."
  type        = string
  default     = "America/Sao_Paulo"
}

variable "schedule_pause_status" {
  description = "Whether the schedule is paused on creation. PAUSED or UNPAUSED."
  type        = string
  default     = "PAUSED"

  validation {
    condition     = contains(["PAUSED", "UNPAUSED"], var.schedule_pause_status)
    error_message = "Must be PAUSED or UNPAUSED."
  }
}
