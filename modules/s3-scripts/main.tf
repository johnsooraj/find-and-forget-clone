# The Python scripts of the Glue jobs, uploaded to the job bucket.
#
# Each script goes under a folder named after its content hash, so a change
# creates a new object and the jobs switch to it atomically. The file name
# itself stays the same because Python imports the modules by name.

terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

locals {
  keys = {
    for name in var.names :
    name => "${var.key_prefix}${filesha256("${var.source_dir}/${name}.py")}/${name}.py"
  }
}

resource "aws_s3_object" "this" {
  for_each = local.keys

  bucket      = var.bucket
  key         = each.value
  source      = "${var.source_dir}/${each.key}.py"
  source_hash = filesha256("${var.source_dir}/${each.key}.py")

  lifecycle {
    precondition {
      condition = var.source_bucket != var.bucket || (
        !startswith(var.job_prefix, var.source_prefix) && !startswith(var.source_prefix, var.job_prefix)
      )
      error_message = "Source objects and Glue job assets must use separate S3 prefixes."
    }
    create_before_destroy = true
  }
}
