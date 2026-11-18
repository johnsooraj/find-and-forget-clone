# An EventBridge rule with one target (used to route Glue run failures to SNS).

terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

resource "aws_cloudwatch_event_rule" "this" {
  name          = var.name
  description   = var.description
  event_pattern = var.event_pattern
}

resource "aws_cloudwatch_event_target" "this" {
  rule = aws_cloudwatch_event_rule.this.name
  arn  = var.target_arn
  # TODO: input_transformer with the common template wording (scripts/notify.py).
}
