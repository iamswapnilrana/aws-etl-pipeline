resource "aws_lambda_function" "move_raw_to_silver" {
  function_name = "move_raw_to_silver"
  role          = aws_iam_role.lambda_role.arn
  handler       = "move_raw_to_silver.lambda_handler"
  runtime       = "python3.11"
  timeout       = 30

  filename = "${path.module}/../lambda_functions/move_raw_to_silver.zip"
}

resource "aws_lambda_function" "redshift_loader" {
  function_name = "redshift_loader"
  role          = aws_iam_role.lambda_role.arn
  handler       = "redshift_loader.lambda_handler"
  runtime       = "python3.11"
  timeout       = 30

  filename = "${path.module}/../lambda_functions/redshift_loader.zip"
}
