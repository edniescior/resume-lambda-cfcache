import json
import os

import boto3
import moto
import pytest
from handler import create_paths_to_invalidate, get_ssm_parameter, lambda_handler


# ===============================================================
# Fixtures
# ===============================================================
@pytest.fixture(scope="session")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"  # noqa
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # noqa
    os.environ["AWS_SECURITY_TOKEN"] = "testing"  # noqa
    os.environ["AWS_SESSION_TOKEN"] = "testing"  # noqa
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def cloudfront_distro():
    home = "tests/unit"
    mock_cf_dist_file = os.path.join(home, "mock_cf_distro.json")
    with open(mock_cf_dist_file, "r") as distro_file:
        yield json.load(distro_file)


@pytest.fixture(scope="function")
def cloudfront_client(aws_credentials, cloudfront_distro):
    with moto.mock_cloudfront():
        client = boto3.client("cloudfront", region_name="us-east-1")
        client.create_distribution(DistributionConfig=cloudfront_distro)
        yield client


@pytest.fixture(scope="function")
def cloudfront_distro_id(cloudfront_client):
    distribution_id = cloudfront_client.list_distributions()["DistributionList"][
        "Items"
    ][0]["Id"]
    return distribution_id


@pytest.fixture(scope="function")
def ssm_client(aws_credentials, cloudfront_distro_id):
    os.environ["CF_DIST_ID_LABEL"] = "/foo/bar"  # noqa
    with moto.mock_ssm():
        client = boto3.client("ssm", region_name="us-east-1")
        client.put_parameter(
            Name=os.environ["CF_DIST_ID_LABEL"],
            Description="A test parameter",
            Value=cloudfront_distro_id,
            Type="SecureString",
        )
        yield client


@pytest.fixture
def eventbridge_single_file_event():
    payload = """
{
    "version": "0",
    "id": "dd5c2fbf-9555-8bf7-2795-23375f6f8bdb",
    "detail-type": "Object Created",
    "source": "aws.s3",
    "account": "12345678910",
    "time": "2024-01-26T19:46:17Z",
    "region": "us-east-1",
    "resources": [
        "arn:aws:s3:::example.com"
    ],
    "detail": {
        "version": "0",
        "bucket": {
            "name": "example.com"
        },
        "object": {
            "key": "test01.foo2.bar.com",
            "size": 10,
            "etag": "b05403212c66bdc8ccc597fedf6cd5fe",
            "version-id": "8TtpHeSRQMOysErMFgieNVxkrgUED_IB",
            "sequencer": "0065B40C0915C35463"
        },
        "request-id": "JH7SKTGA43YMMQ99",
        "requester": "838979457348",
        "source-ip-address": "68.80.121.21",
        "reason": "PutObject"
    }
}
"""
    return json.loads(payload)


# ===============================================================
# Tests
# ===============================================================
# write a test using the fixture eventbridge_single_file_event to test the function create_paths_to_invalidate
def test_create_paths_to_invalidate(
    eventbridge_single_file_event, ssm_client, cloudfront_client
):
    # create a list of paths to invalidate
    paths_to_invalidate = create_paths_to_invalidate(eventbridge_single_file_event)
    # assert that the list contains the expected paths
    assert paths_to_invalidate == ["/test01.foo2.bar.com"]


# write a test to test get ssm parameter function
def test_get_ssm_parameter(ssm_client, cloudfront_distro_id):
    ssm_param = get_ssm_parameter("/foo/bar")
    assert ssm_param == cloudfront_distro_id


# write a test for the lambda handler function
def test_lambda_handler(
    ssm_client, cloudfront_client, cloudfront_distro_id, eventbridge_single_file_event
):
    # call the lambda handler function
    response = lambda_handler(eventbridge_single_file_event, {})
    # assert that the response is a success
    assert response["statusCode"] == 200
    # assert that the response contains the expected message
    assert (
        response["body"]
        == "Cache invalidation triggered for uploaded files: ['/test01.foo2.bar.com']"
    )

    # assert that the paths were invalidated successfully
    cf_invalidation = cloudfront_client.list_invalidations(
        DistributionId=cloudfront_distro_id
    )["InvalidationList"]["Items"][0]
    assert cf_invalidation["Status"] == "COMPLETED"
