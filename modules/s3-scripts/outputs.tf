output "uris" {
  description = "Script name => s3:// URI of the uploaded object."
  value       = { for name, object in aws_s3_object.this : name => "s3://${object.bucket}/${object.key}" }
}
