terraform {
  # 1.10+ for S3 native state locking (use_lockfile in backend/*.s3.tfbackend).
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Partial configuration: the bucket, key and region come from backend/<env>.s3.tfbackend
  #   terraform init -reconfigure -backend-config=backend/stage.s3.tfbackend
  backend "s3" {}
}

provider "aws" {
  region = var.region

  default_tags {
    tags = var.tags
  }
}
