# drop-glue: one module per resource (see modules/).
#
#   module.scripts              modules/s3-scripts        the Python scripts in the job bucket
#   module.tracker              modules/dynamodb-table    processed-file tracker (tracker = DYNAMODB)
#   module.role["find|forget"]  modules/iam-role          one least-privilege role + policy per job
#   module.job["find|forget"]   modules/glue-job          the Glue Spark jobs (+ their log groups)
#   module.source_lifecycle     modules/s3-lifecycle      optional: expire superseded versions
#   module.glue_failure_alerts  modules/eventbridge-rule  optional: failed/killed runs -> SNS (placeholder)
#
# Per environment:  backend/<env>.s3.tfbackend  +  tfvars/<env>.tfvars  (see docs/GUIDE.md, 7.2)

module "scripts" {
  source = "./modules/s3-scripts"

  bucket        = var.job_bucket
  key_prefix    = "${local.job_prefix}scripts/"
  job_prefix    = local.job_prefix
  source_dir    = "${path.root}/scripts"
  names         = local.script_names
  source_bucket = var.source_bucket
  source_prefix = var.source_prefix
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

  name             = "${var.name}-${each.key}"
  read_arns        = concat([local.scripts_arn], local.jobs[each.key].read)
  write_arns       = local.jobs[each.key].write
  extra_statements = local.jobs[each.key].extra_statements
}

module "job" {
  for_each = toset(keys(local.jobs))
  source   = "./modules/glue-job"

  name               = "${var.name}-${each.key}"
  role_arn           = module.role[each.key].arn
  script_uri         = module.scripts.uris[each.key]
  timeout            = local.jobs[each.key].timeout
  worker_type        = var.worker_type
  number_of_workers  = var.default_workers
  fixed_arguments    = local.fixed_arguments
  default_arguments  = local.default_arguments
  log_retention_days = var.log_retention_days

  depends_on = [module.scripts] # the shared modules must exist before the jobs run
}

module "source_lifecycle" {
  # Used with FORGET_STRATEGY=REPLACE_TAG_FOR_LIFECYCLE. WARNING: owns the WHOLE
  # lifecycle configuration of the source bucket (see variables.tf).
  count  = var.manage_source_lifecycle ? 1 : 0
  source = "./modules/s3-lifecycle"

  bucket          = var.source_bucket
  rule_id         = "drop-glue-expire-superseded-versions"
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

  name        = "${var.name}-glue-job-state"
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
