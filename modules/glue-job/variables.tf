variable "name" {
  description = "Name of the Glue job and its IAM role."
  type        = string
}

variable "script_uri" {
  description = "s3:// URI of the job's driver script (find.py or forget.py)."
  type        = string
}

variable "read_arns" {
  description = "Object ARNs the job may read (s3:GetObject)."
  type        = list(string)
}

variable "write_arns" {
  description = "Object ARNs the job may write (s3:PutObject)."
  type        = list(string)
}

variable "extra_statements" {
  description = "Additional IAM policy statements (e.g. prefix-limited ListBucket, DeleteObjectVersion)."
  type        = any
  default     = []
}

variable "fixed_arguments" {
  description = "Glue arguments Jenkins cannot override (region, prefixes, stop switch, extra Python files)."
  type        = map(string)
}

variable "default_arguments" {
  description = "Glue arguments with defaults that Jenkins may override per run (throttling)."
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
