# STAGE variables. Use with the "stage" workspace:
#   terraform workspace select -or-create stage
#   terraform plan -var-file=tfvars/stage.tfvars
# Every optional setting is listed with its default in tfvars/example.tfvars.

environment = "stage" # must equal the workspace (checked by the plan)

region        = "us-east-1" # the provisioning pipeline overrides it with its AWS_REGION env var
source_bucket = "your-stage-data-lake-bucket"
source_prefix = "DBD001/archive/"
job_bucket    = "abcd-job-data" # base name: Terraform CREATES abcd-job-data-stage (bucket names are global)

# Resource names get the workspace postfix:
# drop-glue-find-stage, drop-glue-forget-stage, drop-glue-processed-files-stage
name = "drop-glue"

tags = {
  environment = "stage"
  project     = "drop-glue"
}

# Smaller defaults for stage
default_workers        = 6 # 1 driver + 5 x 4 = 20 files at a time
default_files_per_wave = 20
default_max_waves      = 2

tracker = "DYNAMODB"
