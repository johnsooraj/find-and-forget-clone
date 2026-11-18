output "name" {
  value = aws_glue_job.job.name
}

output "arn" {
  value = aws_glue_job.job.arn
}

output "log_group" {
  description = "CloudWatch log group of the job's continuous (driver/executor) logs."
  value       = aws_cloudwatch_log_group.job.name
}
