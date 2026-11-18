variable "name" {
  description = "Rule name."
  type        = string
}

variable "description" {
  description = "Rule description."
  type        = string
  default     = ""
}

variable "event_pattern" {
  description = "Event pattern (JSON string)."
  type        = string
}

variable "target_arn" {
  description = "Target ARN, e.g. an SNS topic (its policy must allow events.amazonaws.com to publish)."
  type        = string
}
