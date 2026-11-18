# PROD variables. Use with the "prod" workspace:
#   terraform workspace select -or-create prod
#   terraform plan -var-file=tfvars/prod.tfvars
# Every optional setting is listed with its default in tfvars/example.tfvars.

environment = "prod" # must equal the workspace (checked by the plan)

region        = "us-east-1" # the provisioning pipeline overrides it with its AWS_REGION env var
source_bucket = "your-prod-data-lake-bucket"
source_prefix = "DBD001/archive/"
job_bucket    = "abcd-job-data" # base name: Terraform CREATES abcd-job-data-prod (bucket names are global)

# Resource names get the workspace postfix:
# drop-glue-find-prod, drop-glue-forget-prod, drop-glue-processed-files-prod
name = "drop-glue"

tags = {
  environment = "prod"
  project     = "drop-glue"
}

default_workers        = 26 # 1 driver + 25 x 4 = 100 files at a time
default_files_per_wave = 100
default_max_waves      = 5

tracker            = "DYNAMODB"
log_retention_days = 90
