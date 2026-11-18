# Reference: every variable with its default. Copy to tfvars/<env>.tfvars
# (prod, stage and uat exist), pair it with backend/<env>.s3.tfbackend, and run
# it in the workspace of the same name. Every name gets the -<workspace> postfix.

environment   = "prod"                 # must equal terraform.workspace
region        = "us-east-1" # the provisioning pipeline overrides it with its AWS_REGION env var
source_bucket = "your-existing-source-bucket"
source_prefix = "DBD001/archive/"
job_bucket    = "abcd-job-data"        # base name; Terraform CREATES abcd-job-data-<workspace> (globally unique)

# Optional (defaults shown)
# environments           = ["prod", "stage", "uat"]   # workspaces allowed to deploy
# name                   = "drop-glue"   # jobs: drop-glue-find-<workspace>, drop-glue-forget-<workspace>
# tags                   = {}            # e.g. { environment = "prod" } on every resource
#
# Capacity and throttling (Jenkins can override the last three per run)
# worker_type            = "G.1X"        # 4 files at a time per worker
# default_workers        = 26            # 1 driver + 25 x 4 = 100 files at a time
# default_files_per_wave = 100
# default_max_waves      = 5             # 500 files per run, then stop; 0 = no limit
# find_timeout           = 480           # minutes; last-resort automatic stop
# forget_timeout         = 480
#
# Forget strategies (Jenkins FORGET_STRATEGY)
# allow_version_purge     = true        # REPLACE_PURGE_OLD_VERSIONS
# allow_lifecycle_tagging = true        # REPLACE_TAG_FOR_LIFECYCLE
# manage_source_lifecycle = false       # true REPLACES the source bucket's lifecycle rules
# superseded_version_days = 7
#
# Processed-file tracker
# tracker                = "DYNAMODB"   # DYNAMODB | S3_TAG | NONE
# tracker_table_name     = ""           # empty = <name>-processed-files; -<workspace> is added
#
# Job bucket (created by this project)
# report_retention_days              = 0    # expire drop-glue/requests/ and results/ after N days; 0 = never
# job_bucket_noncurrent_version_days = 30   # remove old object versions after N days
# job_bucket_force_destroy           = false # set only by the pipeline for destroy + DESTROY_ALL
#
# Notifications (placeholders)
# notifier               = "LOG"        # LOG | SES | SNS
# notify_to              = ""
# notification_topic_arn = ""           # SNS topic for FAILED/TIMEOUT/STOPPED runs
#
# log_retention_days     = 30            # each job's log group /aws-glue/jobs/<name>-<find|forget>
