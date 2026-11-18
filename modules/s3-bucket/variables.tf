variable "name" {
  description = "Bucket name (globally unique)."
  type        = string
}

variable "force_destroy" {
  description = "Allow Terraform to delete the bucket with all its objects and versions. Set only by the provisioning pipeline when destroying (DESTROY_ALL)."
  type        = bool
  default     = false
}

variable "noncurrent_version_days" {
  description = "Days before an overwritten or deleted object version is removed."
  type        = number
  default     = 30
}

variable "expire_prefixes" {
  description = "Prefixes whose objects expire after expire_days."
  type        = list(string)
  default     = []
}

variable "expire_days" {
  description = "Days after which objects under expire_prefixes expire. 0 = never."
  type        = number
  default     = 0
}
