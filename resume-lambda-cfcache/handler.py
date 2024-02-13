import logging
import os
from datetime import datetime, timezone

import boto3
from lambda_decorators import (
    catch_errors,
    load_json_body,
    with_logging,
    with_ssm_parameters,
)

log_level = os.getenv("LOG_LEVEL", "INFO")

logger = logging.getLogger()
logger.setLevel(log_level)

# The secret to get from SSM Parameter Store - the CloudFront distribution id.
# The secret is managed by Terraform. The same TF module sets the parameter as an
# environment variable when deploying this Lambda function. The with_ssm_parameters
# decorator takes this value as a parameter to get the distribution ID from
# SSM Parameter Store at runtime.
cf_dist_id_label = os.getenv("CF_DIST_ID_LABEL")

_cf_client = None


def get_cf_client():
    global _cf_client
    if _cf_client is None:
        _cf_client = boto3.client("cloudfront")
    return _cf_client


@load_json_body
def get_path_to_invalidate(message):
    """Extract the file path of the triggering file from the event body."""
    logger.debug(f"Message: {message}")
    try:
        path = message["body"]["detail"]["object"]["key"]
        return f"/{path}"
    except KeyError as k:
        logger.error(f"KeyError: {str(k)}")
        raise
    except Exception as e:
        logger.error(f"Error getting paths to invalidate: {e}")
        raise


@with_logging
@catch_errors
@with_ssm_parameters(cf_dist_id_label)
def lambda_handler(event, context):

    # Create a list of file paths to invalidate from the batch of events
    paths_to_invalidate = [
        get_path_to_invalidate(message) for message in event["Records"]
    ]
    logger.info(f"Paths to invalidate: {paths_to_invalidate}")

    # Get distribution ID for your CloudFront distribution from SSM Parameter Store
    distribution_id = cf_dist_id_label and os.getenv(cf_dist_id_label)
    if not distribution_id:
        logger.error(
            f"Distribution ID not found in SSM Parameter Store for label {cf_dist_id_label}"
        )
        raise ValueError("Distribution ID not found in SSM Parameter Store")
    logger.debug(f"Distribution ID: {distribution_id}")

    cloudfront_client = get_cf_client()
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
