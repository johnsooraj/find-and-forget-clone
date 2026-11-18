# The Glue job bucket: scripts, input files, reports and the stop switch.
# The bucket and its settings (versioning, encryption, public access block,
# ownership, lifecycle) are one unit, so they live together in this module.

terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

resource "aws_s3_bucket" "this" {
  bucket = var.name
  # Only true while the provisioning pipeline destroys the environment
  # (DESTROY_ALL): it lets Terraform delete every object version first.
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_versioning" "this" {
  bucket = aws_s3_bucket.this.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  bucket                  = aws_s3_bucket.this.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "this" {
  bucket = aws_s3_bucket.this.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  # Reports hold transaction IDs and hashes: expire them after an agreed period.
  dynamic "rule" {
    for_each = var.expire_days > 0 ? var.expire_prefixes : []
    content {
      id     = "expire-${trim(replace(rule.value, "/", "-"), "-")}"
      status = "Enabled"
      filter {
        prefix = rule.value
      }
      expiration {
        days = var.expire_days
      }
    }
  }

  depends_on = [aws_s3_bucket_versioning.this]
}
