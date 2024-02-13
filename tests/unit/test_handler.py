import json
import os

import boto3
import moto
import pytest
from handler import get_path_to_invalidate, lambda_handler

home = "tests/unit"


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
    with moto.mock_ssm():
        client = boto3.client("ssm", region_name="us-east-1")
        client.put_parameter(
            Name=os.environ["CF_DIST_ID_LABEL"],
            Description="A test parameter",
            Value=cloudfront_distro_id,
            Type="SecureString",
        )
        yield client


# an SQS event containing a single EventBridge event, ie one file was uploaded
@pytest.fixture
def eventbridge_single_file_event():
    mock_event_file = os.path.join(home, "mock_event_single_file.json")
    with open(mock_event_file, "r") as event_file:
        yield json.load(event_file)


# an SQS event containing multiple EventBridge events, ie several files were uploaded
@pytest.fixture
def eventbridge_multi_file_event():
    mock_event_file = os.path.join(home, "mock_event_multi_file.json")
    with open(mock_event_file, "r") as event_file:
        yield json.load(event_file)


# ===============================================================
# Tests
# ===============================================================
# write a test using the fixture eventbridge_single_file_event to test the function create_paths_to_invalidate
def test_get_path_to_invalidate(
    eventbridge_single_file_event, ssm_client, cloudfront_client
):
    message = eventbridge_single_file_event["Records"][0]

    # create a list of paths to invalidate
    paths_to_invalidate = get_path_to_invalidate(message)
    # assert that the list contains the expected paths
    assert paths_to_invalidate == "/test01.foo2.bar.com"


def test_get_path_to_invalidate_no_event(ssm_client, cloudfront_client):
    # assert that the exception is raised with the expected message
    with pytest.raises(KeyError) as excinfo:
        get_path_to_invalidate({})
    assert "body" in str(excinfo.value)


# write a test for the lambda handler function
def test_lambda_handler_single(
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


def test_lambda_handler_multi(
    ssm_client, cloudfront_client, cloudfront_distro_id, eventbridge_multi_file_event
):
    # call the lambda handler function
    response = lambda_handler(eventbridge_multi_file_event, {})
    # assert that the response is a success
    assert response["statusCode"] == 200
    # assert that the response contains the expected message
    assert (
        response["body"]
        == "Cache invalidation triggered for uploaded files: ['/test01.foo2.bar.com', '/tags/test02.foo2.bar.com']"
    )

    # assert that the paths were invalidated successfully
    cf_invalidation = cloudfront_client.list_invalidations(
        DistributionId=cloudfront_distro_id
    )["InvalidationList"]["Items"][0]
    assert cf_invalidation["Status"] == "COMPLETED"


def test_lambda_handler_no_event(ssm_client, cloudfront_client):
    # test that the lambda handler returns a failure response when an invalid event is provided

    # call the lambda handler function
    response = lambda_handler("fibble", {})
    # assert that the response is a success
    assert response["statusCode"] == 400
    # assert that the response contains the expected message
    assert "Unable to process request" in response["body"]
