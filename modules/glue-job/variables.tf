variable "name" {
  description = "Name of the Glue job (also names its log group /aws-glue/jobs/<name>)."
  type        = string
}

variable "role_arn" {
  description = "IAM role the job runs as (modules/iam-role)."
  type        = string
}

variable "script_uri" {
  description = "s3:// URI of the job's driver script (find.py or forget.py)."
  type        = string
}

variable "glue_version" {
  description = "Glue version."
  type        = string
  default     = "5.0"
}

variable "fixed_arguments" {
  description = "Glue arguments Jenkins cannot override (region, prefixes, stop switch, tracker, extra Python files)."
  type        = map(string)
}

variable "default_arguments" {
  description = "Glue arguments with defaults that Jenkins may override per run (throttling, notifier)."
  type        = map(string)
  default     = {}
}

variable "worker_type" {
  description = "Glue worker type, e.g. G.1X (4 vCPU, 16 GB): 4 files at a time per worker."
  type        = string
}

variable "number_of_workers" {
  description = "Default number of workers (including the driver). Jenkins may override per run."
  type        = number
}

variable "timeout" {
  description = "Job run timeout in minutes: the last-resort automatic stop."
  type        = number
}

variable "log_retention_days" {
  description = "Retention of the job's continuous log group."
  type        = number
  default     = 30
}
