# One Glue 5.0 Spark job, and the CloudWatch log group its continuous logs go to.

terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

locals {
  log_group = "/aws-glue/jobs/${var.name}"
}

resource "aws_cloudwatch_log_group" "job" {
  name              = local.log_group
  retention_in_days = var.log_retention_days
}

resource "aws_glue_job" "job" {
  name              = var.name
  role_arn          = var.role_arn
  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  timeout           = var.timeout
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = var.script_uri
  }

  execution_property {
    max_concurrent_runs = 1 # a batch must never be processed by two runs at once
  }

  # Jenkins may override these per run (throttling).
  default_arguments = merge({
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.job.name
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
  }, var.default_arguments)

  # Jenkins can never change these.
  non_overridable_arguments = var.fixed_arguments
}
