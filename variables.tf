variable "region" {
  description = "AWS region of the buckets and Glue jobs."
  type        = string
}

variable "source_bucket" {
  description = "Existing data lake bucket holding the .tar.gz part files."
  type        = string
}

variable "source_prefix" {
  description = "The only prefix the jobs may read and rewrite, e.g. DBD001/archive/."
  type        = string
  default     = "DBD001/archive/"

  validation {
    condition     = can(regex("^[^*?/][^*?]*/$", var.source_prefix))
    error_message = "Source prefix must end in a slash and contain no IAM wildcards."
  }
}

variable "job_bucket" {
  description = "Existing bucket for the scripts, input files, reports and stop switch (under drop-glue/)."
  type        = string
}

variable "name" {
  description = "Prefix for the Glue job and IAM role names."
  type        = string
  default     = "drop-glue"

  validation {
    condition     = can(regex("^[A-Za-z0-9_-]{1,50}$", var.name))
    error_message = "Name must be 1 to 50 letters, digits, underscores or hyphens."
  }
}

variable "tags" {
  description = "Tags added to every resource (provider default_tags), e.g. { environment = \"prod\" }."
  type        = map(string)
  default     = {}
}

# ---------------------------------------------------------------- capacity and throttling
variable "worker_type" {
  description = "Glue worker type. G.1X = 4 vCPU / 16 GB / 64 GB disk = 4 part files at a time per worker."
  type        = string
  default     = "G.1X"
}

variable "default_workers" {
  description = "Default workers per run, including 1 driver. 26 x G.1X = 100 files at a time. Jenkins WORKERS overrides it."
  type        = number
  default     = 26

  validation {
    condition     = var.default_workers >= 2
    error_message = "A Spark job needs at least 2 workers (1 driver + 1 executor)."
  }
}

variable "default_files_per_wave" {
  description = "Default files processed at the same time. Jenkins FILES_PER_WAVE overrides it."
  type        = number
  default     = 100
}

variable "default_max_waves" {
  description = "Default waves per run before it stops cleanly (0 = no limit). Jenkins MAX_WAVES overrides it."
  type        = number
  default     = 5
}

variable "find_timeout" {
  description = "Find run timeout in minutes: the last-resort automatic stop."
  type        = number
  default     = 480
}

variable "forget_timeout" {
  description = "Forget run timeout in minutes: the last-resort automatic stop."
  type        = number
  default     = 480
}

# ---------------------------------------------------------------- deletion and logs
variable "allow_version_purge" {
  description = "Grant the Forget role permission to delete old object versions. The job only purges when FORGET_STRATEGY=REPLACE_PURGE_OLD_VERSIONS."
  type        = bool
  default     = true
}

variable "allow_lifecycle_tagging" {
  description = "Grant the Forget role permission to tag old object versions (FORGET_STRATEGY=REPLACE_TAG_FOR_LIFECYCLE)."
  type        = bool
  default     = true
}

variable "manage_source_lifecycle" {
  description = "Create a lifecycle rule on the SOURCE bucket that expires versions tagged drop-glue-superseded=true. WARNING: replaces every other lifecycle rule of that bucket; keep false if the bucket already has a lifecycle configuration."
  type        = bool
  default     = false
}

variable "superseded_version_days" {
  description = "Days after which a superseded (tagged) old version is deleted by the lifecycle rule."
  type        = number
  default     = 7

  validation {
    condition     = var.superseded_version_days >= 1
    error_message = "superseded_version_days must be at least 1."
  }
}

# ---------------------------------------------------------------- processed-file tracker
variable "tracker" {
  description = "Processed-file tracker: DYNAMODB (a table created here), S3_TAG (a tag on each part file) or NONE."
  type        = string
  default     = "DYNAMODB"

  validation {
    condition     = contains(["DYNAMODB", "S3_TAG", "NONE"], var.tracker)
    error_message = "tracker must be DYNAMODB, S3_TAG or NONE."
  }
}

variable "tracker_table_name" {
  description = "DynamoDB table name for tracker = DYNAMODB. Empty = <name>-processed-files."
  type        = string
  default     = ""
}

# ---------------------------------------------------------------- notifications (placeholder)
variable "notifier" {
  description = "How the jobs send notifications: LOG (print to the job log; default until a transport is chosen), SES or SNS (placeholders)."
  type        = string
  default     = "LOG"

  validation {
    condition     = contains(["LOG", "SES", "SNS"], var.notifier)
    error_message = "notifier must be LOG, SES or SNS."
  }
}

variable "notify_to" {
  description = "Comma-separated recipients (email addresses for SES, a topic ARN for SNS). Placeholder."
  type        = string
  default     = ""
}

variable "notification_topic_arn" {
  description = "SNS topic for Glue run state changes (FAILED/TIMEOUT/STOPPED/ERROR). Empty = no EventBridge rule. Placeholder."
  type        = string
  default     = ""
}

variable "log_retention_days" {
  description = "Retention of each job's own log group /aws-glue/jobs/<name>-<find|forget> (continuous driver/executor logs)."
  type        = number
  default     = 30
}
