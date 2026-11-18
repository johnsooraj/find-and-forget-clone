# A lifecycle rule that expires non-current object versions carrying a tag.
#
# WARNING: aws_s3_bucket_lifecycle_configuration owns the WHOLE lifecycle
# configuration of the bucket and replaces any rules defined elsewhere.

terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "this" {
  bucket = var.bucket

  rule {
    id     = var.rule_id
    status = "Enabled"

    filter {
      and {
        prefix = var.prefix
        tags   = var.tags
      }
    }

    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_days
    }
  }
}
