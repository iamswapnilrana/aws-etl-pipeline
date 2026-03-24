import json
import boto3
import urllib.parse

s3 = boto3.client("s3")
stepfunctions = boto3.client("stepfunctions")

STATE_MACHINE_ARN = "arn:aws:states:us-east-1:493902789652:stateMachine:swapnil-data-pipeline"


def lambda_handler(event, context):
    # Extract bucket + key from S3 event
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    source_key = urllib.parse.unquote_plus(record['s3']['object']['key'])

    # Example: raw/transactions/year=2024/month=03/day=01/file.csv
    print(f"File arrived: {source_key}")

    # Destination in Silver layer
    dest_key = source_key.replace("raw/", "silver/input/")

    # Copy to silver
    s3.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key': source_key},
        Key=dest_key
    )

    print(f"Moved {source_key} → {dest_key}")

    # Start Step Functions workflow
    response = stepfunctions.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        input=json.dumps({
            "silver_key": dest_key,
            "bucket": bucket
        })
    )

    print("Started Step Functions:", response['executionArn'])

    return {"status": "success"}
