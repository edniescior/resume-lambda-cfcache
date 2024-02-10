import json
import logging
import os
from datetime import datetime, timezone
from json import JSONDecodeError

import boto3

log_level = os.getenv("LOG_LEVEL", "INFO")

logger = logging.getLogger()
logger.setLevel(log_level)

_ssm_client = None
_cf_client = None


def get_ssm_client():
    global _ssm_client
    if _ssm_client is None:
        _ssm_client = boto3.client("ssm")
    return _ssm_client


def get_cf_client():
    global _cf_client
    if _cf_client is None:
        _cf_client = boto3.client("cloudfront")
    return _cf_client


# Get the distribution ID from SSM Parameter Store
def get_ssm_parameter(parameter_name):
    ssm_client = get_ssm_client()
    try:
        response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
        return response["Parameter"]["Value"]
    except ssm_client.exceptions.ParameterNotFound:
        logger.error(f"SSM Parameter {parameter_name} not found")
        raise
    except Exception as e:
        logger.error(f"Error getting SSM Parameter {parameter_name}: {e}")
        raise


# write a Function to create a list of paths to invalidate given an SQS message
# containing 1 to many event bridge events.
def create_paths_to_invalidate(event):
    try:
        events = [json.loads(message["body"]) for message in event["Records"]]
        files_uploaded = [f["detail"]["object"]["key"] for f in events]
        return ["/{}".format(file) for file in files_uploaded]
    except JSONDecodeError as j:
        logger.error(f"JSONDecodeError: {str(j)}")
        raise
    except KeyError as k:
        logger.error(f"KeyError: {str(k)}")
        raise
    except Exception as e:
        logger.error(f"Error getting paths to invalidate: {e}")
        raise


def lambda_handler(event, context):
    logger.info(f"Event: {json.dumps(event, indent=2)}")

    # Create a list of file paths to invalidate
    paths_to_invalidate = create_paths_to_invalidate(event)
    logger.info(f"Paths to invalidate: {paths_to_invalidate}")

    # Get distribution ID for your CloudFront distribution from SSM Parameter Store
    distribution_id_parameter_name = os.getenv("CF_DIST_ID_LABEL", "NoParameterSet")
    distribution_id = get_ssm_parameter(distribution_id_parameter_name)
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
