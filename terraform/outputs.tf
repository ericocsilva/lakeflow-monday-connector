output "managed_classic_job_id" {
  description = "Job ID for monday-lakeflow-managed-classic"
  value       = databricks_job.monday_managed_classic.id
}

output "managed_classic_job_url" {
  description = "Workspace URL for monday-lakeflow-managed-classic"
  value       = "${var.databricks_host}/jobs/${databricks_job.monday_managed_classic.id}"
}

output "external_classic_job_id" {
  description = "Job ID for monday-lakeflow-external-classic"
  value       = databricks_job.monday_external_classic.id
}

output "external_classic_job_url" {
  description = "Workspace URL for monday-lakeflow-external-classic"
  value       = "${var.databricks_host}/jobs/${databricks_job.monday_external_classic.id}"
}

output "managed_dynamic_classic_job_id" {
  description = "Job ID for monday-lakeflow-managed-dynamic-classic"
  value       = databricks_job.monday_managed_dynamic_classic.id
}

output "managed_dynamic_classic_job_url" {
  description = "Workspace URL for monday-lakeflow-managed-dynamic-classic"
  value       = "${var.databricks_host}/jobs/${databricks_job.monday_managed_dynamic_classic.id}"
}

output "external_foreach_classic_job_id" {
  description = "Job ID for monday-lakeflow-external-foreach-classic"
  value       = databricks_job.monday_external_foreach_classic.id
}

output "external_foreach_classic_job_url" {
  description = "Workspace URL for monday-lakeflow-external-foreach-classic"
  value       = "${var.databricks_host}/jobs/${databricks_job.monday_external_foreach_classic.id}"
}

output "external_dynamic_classic_job_id" {
  description = "Job ID for monday-lakeflow-external-dynamic-classic"
  value       = databricks_job.monday_external_dynamic_classic.id
}

output "external_dynamic_classic_job_url" {
  description = "Workspace URL for monday-lakeflow-external-dynamic-classic"
  value       = "${var.databricks_host}/jobs/${databricks_job.monday_external_dynamic_classic.id}"
}
