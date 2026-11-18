# UAT variables. Use with the "uat" workspace:
#   terraform workspace select -or-create uat
#   terraform plan -var-file=tfvars/uat.tfvars
# Every optional setting is listed with its default in tfvars/example.tfvars.

environment = "uat" # must equal the workspace (checked by the plan)

region        = "us-east-1" # the provisioning pipeline overrides it with its AWS_REGION env var
source_bucket = "your-uat-data-lake-bucket"
source_prefix = "DBD001/archive/"
job_bucket    = "abcd-job-data" # base name: Terraform CREATES abcd-job-data-uat (bucket names are global)

# Resource names get the workspace postfix:
# drop-glue-find-uat, drop-glue-forget-uat, drop-glue-processed-files-uat
name = "drop-glue"

tags = {
  environment = "uat"
  project     = "drop-glue"
}

default_workers        = 11 # 1 driver + 10 x 4 = 40 files at a time
default_files_per_wave = 40
default_max_waves      = 5

tracker = "DYNAMODB"
