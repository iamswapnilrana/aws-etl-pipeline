terraform {
  backend "s3" {
    bucket         = "swapnil-terraform-state-493902789652"
    key            = "aws-etl-pipeline/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "swapnil-terraform-locks"
    encrypt        = true
  }
}