variable "bucket" {
  description = "Bucket whose lifecycle configuration this module owns."
  type        = string
}

variable "rule_id" {
  description = "Lifecycle rule ID."
  type        = string
}

variable "prefix" {
  description = "Only objects under this prefix."
  type        = string
}

variable "tags" {
  description = "Only object versions carrying all of these tags."
  type        = map(string)
}

variable "noncurrent_days" {
  description = "Days after becoming non-current before a version is deleted."
  type        = number
}
