# Reference: every variable with its default. Copy to tfvars/<env>.tfvars
# (stage.tfvars and prod.tfvars exist) and pair it with backend/<env>.s3.tfbackend.

region        = "us-east-1"
source_bucket = "your-existing-source-bucket"
source_prefix = "DBD001/archive/"
job_bucket    = "your-existing-glue-job-bucket"

# Optional (defaults shown)
# name                   = "drop-glue"   # jobs: drop-glue-find, drop-glue-forget; make it unique per env
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
# tracker_table_name     = ""           # empty = <name>-processed-files
#
# Notifications (placeholders)
# notifier               = "LOG"        # LOG | SES | SNS
# notify_to              = ""
# notification_topic_arn = ""           # SNS topic for FAILED/TIMEOUT/STOPPED runs
#
# log_retention_days     = 30            # each job's log group /aws-glue/jobs/<name>-<find|forget>
