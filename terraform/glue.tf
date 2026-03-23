resource "aws_glue_job" "raw_to_silver" {
  name     = "raw_to_silver"
  role_arn = aws_iam_role.glue_role.arn
  command {
    script_location = "s3://swapnil-data-lake/scripts/raw_to_silver.py"
    python_version  = "3"
  }
}

resource "aws_glue_job" "silver_to_gold" {
  name     = "silver_to_gold"
  role_arn = aws_iam_role.glue_role.arn
  command {
    script_location = "s3://swapnil-data-lake/scripts/silver_to_gold.py"
    python_version  = "3"
  }
}