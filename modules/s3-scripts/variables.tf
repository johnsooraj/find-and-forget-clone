variable "bucket" {
  description = "Job bucket the scripts are uploaded to."
  type        = string
}

variable "key_prefix" {
  description = "Key prefix for the scripts, ending in a slash, e.g. drop-glue/scripts/."
  type        = string
}

variable "source_dir" {
  description = "Local folder holding the <name>.py files."
  type        = string
}

variable "names" {
  description = "Script names without .py."
  type        = list(string)
}

variable "job_prefix" {
  description = "Root prefix of every Glue job asset in the job bucket, e.g. drop-glue/ (must not overlap the source prefix)."
  type        = string
}

variable "source_bucket" {
  description = "Data lake bucket (only used to check the prefixes don't overlap)."
  type        = string
}

variable "source_prefix" {
  description = "Data lake prefix (only used to check the prefixes don't overlap)."
  type        = string
}
