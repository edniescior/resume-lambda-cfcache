import json
import logging
import os
from datetime import datetime, timezone

import boto3

log_level = os.getenv("LOG_LEVEL", "INFO")

logger = logging.getLogger()
logger.setLevel(log_level)


# Get the distribution ID from SSM Parameter Store
def get_ssm_parameter(parameter_name):
    ssm_client = boto3.client("ssm")
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    return response["Parameter"]["Value"]


# write a Function to create a list of paths to invalidate given an SQS message
# containing 1 to many event bridge events.
def create_paths_to_invalidate(event):
    events = [json.loads(message["body"]) for message in event["Records"]]
    files_uploaded = [event["detail"]["object"]["key"] for event in events]
    paths_to_invalidate = ["/{}".format(file) for file in files_uploaded]
    return paths_to_invalidate


def lambda_handler(event, context):
    logger.info(f"Event: {json.dumps(event, indent=2)}")

    # Create a list of file paths to invalidate
    paths_to_invalidate = create_paths_to_invalidate(event)
    logger.info(f"Paths to invalidate: {paths_to_invalidate}")

    # Get distribution ID for your CloudFront distribution from SSM Parameter Store
    distribution_id_parameter_name = os.getenv("CF_DIST_ID_LABEL", "NoParameterSet")
    distribution_id = get_ssm_parameter(distribution_id_parameter_name)
    logger.debug(f"Distribution ID: {distribution_id}")

    cloudfront_client = boto3.client("cloudfront")
    # Create CloudFront invalidation request
    dt_now = datetime.now(tz=timezone.utc)
    response = cloudfront_client.create_invalidation(
        DistributionId=distribution_id,
        InvalidationBatch={
            "Paths": {
                "Quantity": len(paths_to_invalidate),
                "Items": paths_to_invalidate,
            },
            "CallerReference": f"{dt_now=:%Y-%m-%d %H:%M:%S}",
        },
    )
    logger.debug(f"Invalidation response: {response}")

    return {
        "statusCode": 200,
        "body": f"Cache invalidation triggered for uploaded files: {paths_to_invalidate}",
    }
