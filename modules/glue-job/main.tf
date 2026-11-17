terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

data "aws_partition" "current" {}
data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

locals {
  log_groups = "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"
}

resource "aws_iam_role" "job" {
  name = var.name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "job" {
  role = aws_iam_role.job.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [
        {
          Effect   = "Allow"
          Action   = ["s3:GetObject"]
          Resource = var.read_arns
        },
        {
          Effect   = "Allow"
          Action   = ["s3:PutObject"]
          Resource = var.write_arns
        },
        {
          Effect   = "Allow"
          Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:AssociateKmsKey"]
          Resource = [local.log_groups, "${local.log_groups}:*"]
        },
        {
          Effect    = "Allow"
          Action    = ["cloudwatch:PutMetricData"]
          Resource  = "*"
          Condition = { StringEquals = { "cloudwatch:namespace" = "Glue" } }
        }
      ],
      var.extra_statements
    )
  })
}

resource "aws_glue_job" "job" {
  name              = var.name
  role_arn          = aws_iam_role.job.arn
  glue_version      = "5.0"
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
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
  }, var.default_arguments)

  # Jenkins can never change these.
  non_overridable_arguments = var.fixed_arguments

  depends_on = [aws_iam_role_policy.job]
}

output "name" {
  value = aws_glue_job.job.name
}

output "role_arn" {
  value = aws_iam_role.job.arn
}
