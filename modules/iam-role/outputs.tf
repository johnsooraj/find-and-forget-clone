output "name" {
  value = aws_iam_role.this.name
}

output "arn" {
  # Depends on the policy, so a job created with this ARN never runs without its permissions.
  value      = aws_iam_role.this.arn
  depends_on = [aws_iam_role_policy.this]
}
