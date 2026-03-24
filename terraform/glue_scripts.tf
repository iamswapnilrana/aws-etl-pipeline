resource "aws_s3_object" "raw_to_silver_script" {
  bucket = aws_s3_bucket.data_lake.bucket
  key    = "scripts/raw_to_silver.py"
  source = "${path.module}/../glue_jobs/raw_to_silver.py"
  etag   = filemd5("${path.module}/../glue_jobs/raw_to_silver.py")
}

resource "aws_s3_object" "silver_to_gold_script" {
  bucket = aws_s3_bucket.data_lake.bucket
  key    = "scripts/silver_to_gold.py"
  source = "${path.module}/../glue_jobs/silver_to_gold.py"
  etag   = filemd5("${path.module}/../glue_jobs/silver_to_gold.py")
}