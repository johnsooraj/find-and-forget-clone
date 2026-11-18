# An on-demand DynamoDB table with string keys (used by the processed-file tracker).

terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

resource "aws_dynamodb_table" "this" {
  name         = var.name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = var.hash_key
  range_key    = var.range_key

  attribute {
    name = var.hash_key
    type = "S"
  }
  attribute {
    name = var.range_key
    type = "S"
  }

  point_in_time_recovery {
    enabled = var.point_in_time_recovery
  }
  server_side_encryption {
    enabled = true
  }
}
