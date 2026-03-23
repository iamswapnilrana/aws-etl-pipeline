import boto3
import os

redshift_data = boto3.client("redshift-data")

DATABASE = "dev"
DB_USER = "awsuser"
CLUSTER_ID = "your-redshift-serverless-workgroup"
IAM_ROLE_ARN = "arn:aws:iam::493902789652:role/lambda_data_pipeline_role"

def lambda_handler(event, context):
    s3_path = event["gold_path"]

    sql = f"""
        COPY fact_transactions
        FROM '{s3_path}'
        IAM_ROLE '{IAM_ROLE_ARN}'
        FORMAT AS PARQUET;
    """

    response = redshift_data.execute_statement(
        ClusterIdentifier=CLUSTER_ID,
        Database=DATABASE,
        DbUser=DB_USER,
        Sql=sql
    )

    print("Redshift COPY started:", response)
    return {"status": "Redshift load started"}