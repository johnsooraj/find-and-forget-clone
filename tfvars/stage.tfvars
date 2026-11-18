# STAGE variables.  terraform plan -var-file=tfvars/stage.tfvars
# Every optional setting is listed with its default in tfvars/example.tfvars.

region        = "us-east-1"
source_bucket = "your-stage-data-lake-bucket"
source_prefix = "DBD001/archive/"
job_bucket    = "your-stage-glue-job-bucket"

# Unique per environment when stage and prod share an AWS account
# (jobs: drop-glue-stage-find / -forget, table: drop-glue-stage-processed-files).
name = "drop-glue-stage"

tags = {
  environment = "stage"
  project     = "drop-glue"
}

# Smaller defaults for stage
default_workers        = 6 # 1 driver + 5 x 4 = 20 files at a time
default_files_per_wave = 20
default_max_waves      = 2

tracker = "DYNAMODB"
