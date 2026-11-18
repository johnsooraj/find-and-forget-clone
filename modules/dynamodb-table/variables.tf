variable "name" {
  description = "Table name."
  type        = string
}

variable "hash_key" {
  description = "Partition key (string)."
  type        = string
}

variable "range_key" {
  description = "Sort key (string)."
  type        = string
}

variable "point_in_time_recovery" {
  description = "Keep 35 days of point-in-time backups."
  type        = bool
  default     = true
}
