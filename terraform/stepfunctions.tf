resource "aws_sfn_state_machine" "pipeline" {
  name     = "swapnil-data-pipeline"
  role_arn = aws_iam_role.sfn_role.arn

  definition = <<EOF
{
  "StartAt": "RawToSilver",
  "States": {
    "RawToSilver": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "raw_to_silver"
      },
      "Next": "SilverToGold"
    },
    "SilverToGold": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "silver_to_gold"
      },
      "Next": "LoadToRedshift"
    },
    "LoadToRedshift": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.redshift_loader.arn}",
      "End": true
    }
  }
}
EOF
}