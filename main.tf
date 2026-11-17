terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region
}

data "aws_partition" "current" {}

locals {
  s3_arn            = "arn:${data.aws_partition.current.partition}:s3:::"
  source_bucket_arn = "${local.s3_arn}${var.source_bucket}"
  job_bucket_arn    = "${local.s3_arn}${var.job_bucket}"
  source_arn        = "${local.source_bucket_arn}/${var.source_prefix}*"
  request_arn       = "${local.job_bucket_arn}/drop-glue/requests/*"
  result_arn        = "${local.job_bucket_arn}/drop-glue/results/*"
  scripts_arn       = "${local.job_bucket_arn}/drop-glue/scripts/*"
  result_prefix     = "s3://${var.job_bucket}/drop-glue/results/"
  stop_uri          = "s3://${var.job_bucket}/drop-glue/control/STOP"

  # Each script is uploaded under a folder named after its content hash, so a
  # change creates a new object and the jobs switch to it atomically. The file
  # name itself stays the same because Python imports the modules by name.
  script_names = ["find", "forget", "common", "formats", "tasks"]
  script_keys = {
    for name in local.script_names :
    name => "drop-glue/scripts/${filesha256("${path.module}/scripts/${name}.py")}/${name}.py"
  }
  # Modules shipped to the driver and every Spark executor.
  extra_py_files = join(",", [
    for name in ["common", "formats", "tasks"] : "s3://${var.job_bucket}/${local.script_keys[name]}"
  ])

  # Listing is limited by an s3:prefix condition.
  list_job_bucket = {
    Effect    = "Allow"
    Action    = ["s3:ListBucket"]
    Resource  = local.job_bucket_arn
    Condition = { StringLike = { "s3:prefix" = ["drop-glue/results/*", "drop-glue/control/*"] } }
  }
  list_source = {
    Effect    = "Allow"
    Action    = ["s3:ListBucket"]
    Resource  = local.source_bucket_arn
    Condition = { StringLike = { "s3:prefix" = ["${var.source_prefix}*"] } }
  }
  purge_statements = [
    {
      Effect    = "Allow"
      Action    = ["s3:ListBucketVersions"]
      Resource  = local.source_bucket_arn
      Condition = { StringLike = { "s3:prefix" = ["${var.source_prefix}*"] } }
    },
    {
      Effect   = "Allow"
      Action   = ["s3:DeleteObjectVersion"]
      Resource = local.source_arn
    }
  ]

  jobs = {
    find = {
      read             = [local.source_arn, local.request_arn, local.result_arn]
      write            = [local.result_arn]
      extra_statements = [local.list_source, local.list_job_bucket]
      timeout          = var.find_timeout
    }
    forget = {
      read             = [local.source_arn, local.result_arn]
      write            = [local.source_arn, local.result_arn]
      extra_statements = concat([local.list_job_bucket], slice(local.purge_statements, 0, var.allow_version_purge ? 2 : 0))
      timeout          = var.forget_timeout
    }
  }
}

resource "aws_s3_object" "scripts" {
  for_each = local.script_keys

  bucket      = var.job_bucket
  key         = each.value
  source      = "${path.module}/scripts/${each.key}.py"
  source_hash = filesha256("${path.module}/scripts/${each.key}.py")

  lifecycle {
    precondition {
      condition = var.source_bucket != var.job_bucket || (
        !startswith("drop-glue/", var.source_prefix) && !startswith(var.source_prefix, "drop-glue/")
      )
      error_message = "Source objects and Glue job assets must use separate S3 prefixes."
    }
    create_before_destroy = true
  }
}

module "job" {
  # The two jobs have differently shaped policies, so iterate over the names.
  for_each = toset(keys(local.jobs))

  source            = "./modules/glue-job"
  name              = "${var.name}-${each.key}"
  script_uri        = "s3://${aws_s3_object.scripts[each.key].bucket}/${aws_s3_object.scripts[each.key].key}"
  read_arns         = concat([local.scripts_arn], local.jobs[each.key].read)
  write_arns        = local.jobs[each.key].write
  extra_statements  = local.jobs[each.key].extra_statements
  timeout           = local.jobs[each.key].timeout
  worker_type       = var.worker_type
  number_of_workers = var.default_workers

  fixed_arguments = {
    "--region"         = var.region
    "--result_prefix"  = local.result_prefix
    "--source_prefix"  = "s3://${var.source_bucket}/${var.source_prefix}"
    "--stop_uri"       = local.stop_uri
    "--extra-py-files" = local.extra_py_files
  }
  default_arguments = {
    "--files_per_wave" = tostring(var.default_files_per_wave)
    "--max_waves"      = tostring(var.default_max_waves)
  }

  depends_on = [aws_s3_object.scripts] # the shared modules must exist before the jobs run
}

# Optional: these log groups are shared by every Glue Spark job in the
# account, so only manage them here if nothing else does.
resource "aws_cloudwatch_log_group" "glue" {
  for_each          = var.manage_log_groups ? toset(["output", "error", "logs-v2"]) : toset([])
  name              = "/aws-glue/jobs/${each.key}"
  retention_in_days = var.log_retention_days
}

output "job_names" {
  description = "Glue job names to configure in the Jenkins pipelines."
  value       = { for phase, job in module.job : phase => job.name }
}

output "role_arns" {
  description = "IAM roles the Glue jobs run as (add to bucket / KMS key policies if needed)."
  value       = { for phase, job in module.job : phase => job.role_arn }
}

output "request_prefix" {
  description = "Where Jenkins (or people) upload input files."
  value       = "s3://${var.job_bucket}/drop-glue/requests/"
}

output "result_prefix" {
  description = "Where reports are written: <result_prefix><BATCH_ID>/"
  value       = local.result_prefix
}

output "stop_uri" {
  description = "The emergency stop switch object used by Jenkinsfile.stop."
  value       = local.stop_uri
}
