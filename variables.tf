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
  description = "Grant the Forget role permission to delete old object versions. The job only purges when PURGE_OLD_VERSIONS=true."
  type        = bool
  default     = true
}

variable "manage_log_groups" {
  description = "Create the shared /aws-glue/jobs/{output,error,logs-v2} log groups with a retention period. Leave false if they already exist."
  type        = bool
  default     = false
}

variable "log_retention_days" {
  description = "Log retention when manage_log_groups is true."
  type        = number
  default     = 30
}
