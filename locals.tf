data "aws_partition" "current" {}

locals {
  s3_arn            = "arn:${data.aws_partition.current.partition}:s3:::"
  source_bucket_arn = "${local.s3_arn}${var.source_bucket}"
  job_bucket_arn    = "${local.s3_arn}${var.job_bucket}"
  job_prefix        = "drop-glue/"
  source_arn        = "${local.source_bucket_arn}/${var.source_prefix}*"
  request_arn       = "${local.job_bucket_arn}/${local.job_prefix}requests/*"
  result_arn        = "${local.job_bucket_arn}/${local.job_prefix}results/*"
  scripts_arn       = "${local.job_bucket_arn}/${local.job_prefix}scripts/*"
  result_prefix     = "s3://${var.job_bucket}/${local.job_prefix}results/"
  stop_uri          = "s3://${var.job_bucket}/${local.job_prefix}control/STOP"

  # ------------------------------------------------------------ scripts
  #   layer 1: arguments   layer 2: find, find_strategies   layer 3: forget, forget_strategies
  #   shared:  common, formats, tasks, tracking, notify
  shared_modules = ["arguments", "common", "formats", "tasks", "find_strategies", "forget_strategies", "tracking", "notify"]
  script_names   = concat(["find", "forget"], local.shared_modules)
  # Modules shipped to the driver and every Spark executor.
  extra_py_files = join(",", [for name in local.shared_modules : module.scripts.uris[name]])

  # ------------------------------------------------------------ S3 permissions
  # Listing is limited by an s3:prefix condition.
  list_job_bucket = {
    Effect    = "Allow"
    Action    = ["s3:ListBucket"]
    Resource  = local.job_bucket_arn
    Condition = { StringLike = { "s3:prefix" = ["${local.job_prefix}results/*", "${local.job_prefix}control/*"] } }
  }
  list_source = {
    Effect    = "Allow"
    Action    = ["s3:ListBucket"]
    Resource  = local.source_bucket_arn
    Condition = { StringLike = { "s3:prefix" = ["${var.source_prefix}*"] } }
  }
  purge_statements = var.allow_version_purge ? [
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
  ] : []
  # REPLACE_TAG_FOR_LIFECYCLE tags the superseded (old) version of a part file.
  lifecycle_tag_statements = var.allow_lifecycle_tagging ? [{
    Effect   = "Allow"
    Action   = ["s3:GetObjectVersionTagging", "s3:PutObjectVersionTagging"]
    Resource = local.source_arn
  }] : []

  # ------------------------------------------------------------ processed-file tracker
  tracker_table = var.tracker_table_name != "" ? var.tracker_table_name : "${var.name}-processed-files"
  tracker_arguments = {
    for key, value in {
      "--tracker"       = var.tracker
      "--tracker_table" = var.tracker == "DYNAMODB" ? local.tracker_table : ""
    } : key => value if value != ""
  }
  tracker_statements = concat(
    var.tracker == "DYNAMODB" ? [{
      Effect   = "Allow"
      Action   = ["dynamodb:BatchGetItem", "dynamodb:BatchWriteItem", "dynamodb:GetItem", "dynamodb:PutItem"]
      Resource = one(module.tracker[*].arn)
    }] : [],
    var.tracker == "S3_TAG" ? [{
      Effect   = "Allow"
      Action   = ["s3:GetObjectTagging", "s3:PutObjectTagging", "s3:GetObjectVersionTagging", "s3:PutObjectVersionTagging"]
      Resource = local.source_arn
    }] : []
  )

  # ------------------------------------------------------------ the two jobs
  jobs = {
    find = {
      read             = [local.source_arn, local.request_arn, local.result_arn]
      write            = [local.result_arn]
      extra_statements = concat([local.list_source, local.list_job_bucket], local.tracker_statements)
      timeout          = var.find_timeout
    }
    forget = {
      read  = [local.source_arn, local.result_arn]
      write = [local.source_arn, local.result_arn]
      extra_statements = concat(
        [local.list_job_bucket],
        local.purge_statements,
        local.lifecycle_tag_statements,
        local.tracker_statements
      )
      timeout = var.forget_timeout
    }
  }

  fixed_arguments = merge({
    "--region"         = var.region
    "--result_prefix"  = local.result_prefix
    "--source_prefix"  = "s3://${var.source_bucket}/${var.source_prefix}"
    "--stop_uri"       = local.stop_uri
    "--extra-py-files" = local.extra_py_files
  }, local.tracker_arguments)

  default_arguments = {
    for key, value in {
      "--files_per_wave" = tostring(var.default_files_per_wave)
      "--max_waves"      = tostring(var.default_max_waves)
      "--notifier"       = var.notifier
      "--notify_to"      = var.notify_to
    } : key => value if value != ""
  }
}
