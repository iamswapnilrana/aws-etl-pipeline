import boto3

redshift_data = boto3.client("redshift-data", region_name="us-east-1")

DATABASE = "dev"
WORKGROUP_NAME = "swapnil-redshift-wg"
IAM_ROLE_ARN = "arn:aws:iam::493902789652:role/lambda_data_pipeline_role"


def lambda_handler(event, context):
    # The Step Function passes the gold path
    s3_path = event["gold_path"]

    # Redshift COPY command for serverless
    sql = f"""
        COPY fact_transactions
        FROM '{s3_path}'
        IAM_ROLE '{IAM_ROLE_ARN}'
        FORMAT AS PARQUET
        INCLUDE '.*\\.parquet';
    """
    response = redshift_data.execute_statement(
        WorkgroupName=WORKGROUP_NAME,
        Database=DATABASE,
        Sql=sql,
        WithEvent=True
    )

    print("Redshift COPY started:", response)
    return {"status": "Redshift load started"}
