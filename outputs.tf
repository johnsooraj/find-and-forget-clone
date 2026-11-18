output "environment" {
  description = "The workspace: the -<postfix> of every resource name in this deployment."
  value       = local.environment

  # A failed precondition fails the plan, so nothing is created under a wrong name.
  precondition {
    condition     = contains(var.environments, terraform.workspace)
    error_message = "Workspace '${terraform.workspace}' is not one of ${join(", ", var.environments)}. Run: terraform workspace select -or-create <env> (the default workspace is not allowed)."
  }
  precondition {
    condition     = var.environment == terraform.workspace
    error_message = "The tfvars file is for '${var.environment}' but the workspace is '${terraform.workspace}'. Use -var-file=tfvars/${terraform.workspace}.tfvars or select the matching workspace."
  }
  precondition {
    condition     = var.source_bucket != local.job_bucket
    error_message = "The archive bucket and the job bucket must be different buckets."
  }
}

output "region" {
  description = "Region of the deployment (used by the provisioning pipeline to copy the scripts)."
  value       = var.region
}

output "job_bucket" {
  description = "The job bucket created for this environment (<job_bucket>-<workspace>)."
  value       = module.job_bucket.name
}

output "scripts_prefix" {
  description = "Where the provisioning pipeline copies scripts/*.py (COPY_SCRIPTS); the jobs load them from here."
  value       = local.scripts_prefix
}

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
  value       = "s3://${local.job_bucket}/${local.job_prefix}requests/"
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
