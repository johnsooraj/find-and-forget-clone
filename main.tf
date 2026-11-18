# drop-glue: one module per resource (see modules/).
#
#   module.job_bucket           modules/s3-bucket         the job bucket <job_bucket>-<workspace>
#   module.tracker              modules/dynamodb-table    processed-file tracker (tracker = DYNAMODB)
#   module.role["find|forget"]  modules/iam-role          one least-privilege role + policy per job
#   module.job["find|forget"]   modules/glue-job          the Glue Spark jobs (+ their log groups)
#   module.source_lifecycle     modules/s3-lifecycle      optional: expire superseded versions
#   module.glue_failure_alerts  modules/eventbridge-rule  optional: failed/killed runs -> SNS (placeholder)
#
# Per environment:  workspace <env>  +  backend/<env>.s3.tfbackend  +  tfvars/<env>.tfvars  (docs/GUIDE.md, 7.2)
# Every name ends in -${terraform.workspace} (local.suffix in locals.tf), incl. the job bucket.
# Provisioned by ./Jenkinsfile, which also copies scripts/*.py into the job bucket (COPY_SCRIPTS).

module "job_bucket" {
  source = "./modules/s3-bucket"

  name                    = local.job_bucket
  force_destroy           = var.job_bucket_force_destroy
  noncurrent_version_days = var.job_bucket_noncurrent_version_days
  expire_prefixes         = ["${local.job_prefix}requests/", "${local.job_prefix}results/"]
  expire_days             = var.report_retention_days
}

module "tracker" {
  count  = var.tracker == "DYNAMODB" ? 1 : 0
  source = "./modules/dynamodb-table"

  name      = local.tracker_table
  hash_key  = "file_uri"
  range_key = "scope" # "<find strategy>#<fingerprint of settings + input>"
}

module "role" {
  # The two roles have differently shaped policies, so iterate over the names.
  for_each = toset(keys(local.jobs))
  source   = "./modules/iam-role"

  name             = "${var.name}-${each.key}${local.suffix}"
  read_arns        = concat([local.scripts_arn], local.jobs[each.key].read)
  write_arns       = local.jobs[each.key].write
  extra_statements = local.jobs[each.key].extra_statements
}

module "job" {
  for_each = toset(keys(local.jobs))
  source   = "./modules/glue-job"

  name               = "${var.name}-${each.key}${local.suffix}"
  role_arn           = module.role[each.key].arn
  script_uri         = "${local.scripts_prefix}${each.key}.py"
  timeout            = local.jobs[each.key].timeout
  worker_type        = var.worker_type
  number_of_workers  = var.default_workers
  fixed_arguments    = local.fixed_arguments
  default_arguments  = local.default_arguments
  log_retention_days = var.log_retention_days

  depends_on = [module.job_bucket]
}

module "source_lifecycle" {
  # Used with FORGET_STRATEGY=REPLACE_TAG_FOR_LIFECYCLE. WARNING: owns the WHOLE
  # lifecycle configuration of the source bucket (see variables.tf).
  count  = var.manage_source_lifecycle ? 1 : 0
  source = "./modules/s3-lifecycle"

  bucket          = var.source_bucket
  rule_id         = "drop-glue-expire-superseded-versions${local.suffix}"
  prefix          = var.source_prefix
  tags            = { "drop-glue-superseded" = "true" }
  noncurrent_days = var.superseded_version_days
}

module "glue_failure_alerts" {
  # PLACEHOLDER. A killed, timed-out or crashed run cannot notify itself, so
  # Glue's own state change events go to an SNS topic. TODO: the topic (and
  # its policy allowing events.amazonaws.com to publish) and the wording.
  count  = var.notification_topic_arn == "" ? 0 : 1
  source = "./modules/eventbridge-rule"

  name        = "${var.name}-job-state${local.suffix}"
  description = "drop-glue Find/Forget runs that FAILED, TIMED OUT, were STOPPED or hit an ERROR"
  target_arn  = var.notification_topic_arn
  event_pattern = jsonencode({
    source        = ["aws.glue"]
    "detail-type" = ["Glue Job State Change"]
    detail = {
      jobName = [for job in module.job : job.name]
      state   = ["FAILED", "TIMEOUT", "STOPPED", "ERROR"]
    }
  })
}
