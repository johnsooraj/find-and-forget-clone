output "job_names" {
  description = "Glue job names to configure in the Jenkins pipelines."
  value       = { for phase, job in module.job : phase => job.name }
}

output "log_groups" {
  description = "CloudWatch log groups of the jobs' continuous logs."
  value       = { for phase, job in module.job : phase => job.log_group }
}

output "role_arns" {
  description = "IAM roles the Glue jobs run as (add to bucket / KMS key policies if needed)."
  value       = { for phase, role in module.role : phase => role.arn }
}

output "request_prefix" {
  description = "Where Jenkins (or people) upload input files."
  value       = "s3://${var.job_bucket}/${local.job_prefix}requests/"
}

output "result_prefix" {
  description = "Where reports are written: <result_prefix><BATCH_ID>/"
  value       = local.result_prefix
}

output "stop_uri" {
  description = "The emergency stop switch object used by Jenkinsfile.stop."
  value       = local.stop_uri
}

output "tracker_table" {
  description = "DynamoDB table of the processed-file tracker (null unless tracker = DYNAMODB)."
  value       = one(module.tracker[*].name)
}
