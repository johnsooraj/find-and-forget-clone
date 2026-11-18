variable "name" {
  description = "Name of the IAM role and its inline policy."
  type        = string
}

variable "service" {
  description = "AWS service allowed to assume the role."
  type        = string
  default     = "glue.amazonaws.com"
}

variable "read_arns" {
  description = "Object ARNs the role may read (s3:GetObject)."
  type        = list(string)
}

variable "write_arns" {
  description = "Object ARNs the role may write (s3:PutObject)."
  type        = list(string)
}

variable "extra_statements" {
  description = "Additional IAM policy statements (e.g. prefix-limited ListBucket, DeleteObjectVersion, DynamoDB)."
  type        = any
  default     = []
}
