# Keeps an existing deployment (made with an older layout) from being
# destroyed and recreated. Safe to delete once every environment has been
# applied with this layout.

# The shared /aws-glue/jobs/{output,error,logs-v2} log groups (old manage_log_groups)
# are used by every Glue job in the account: forget them, never delete them.
# Each job now has its own log group in modules/glue-job.
removed {
  from = aws_cloudwatch_log_group.glue

  lifecycle {
    destroy = false
  }
}

# Scripts are no longer Terraform objects: the provisioning pipeline copies them
# (./Jenkinsfile, COPY_SCRIPTS). Forget the old objects without deleting them.
removed {
  from = aws_s3_object.scripts

  lifecycle {
    destroy = false
  }
}

removed {
  from = module.scripts

  lifecycle {
    destroy = false
  }
}

moved {
  from = module.job["find"].aws_iam_role.job
  to   = module.role["find"].aws_iam_role.this
}

moved {
  from = module.job["forget"].aws_iam_role.job
  to   = module.role["forget"].aws_iam_role.this
}

moved {
  from = module.job["find"].aws_iam_role_policy.job
  to   = module.role["find"].aws_iam_role_policy.this
}

moved {
  from = module.job["forget"].aws_iam_role_policy.job
  to   = module.role["forget"].aws_iam_role_policy.this
}
