# PROD variables.  terraform plan -var-file=tfvars/prod.tfvars
# Every optional setting is listed with its default in tfvars/example.tfvars.

region        = "us-east-1"
source_bucket = "your-prod-data-lake-bucket"
source_prefix = "DBD001/archive/"
job_bucket    = "your-prod-glue-job-bucket"

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
