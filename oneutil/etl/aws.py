import boto3
import os

from oneutil.logging import logger

def get_s3_bucket_files(
    bucket: str = "onesquared-databento",
    region: str = "us-east-1",
    public_key: str = os.environ.get("s3_public_key"),
    private_key: str = os.environ.get("s3_private_key"),
):
    """
    Get a list of file keys from a specified AWS S3 bucket.

    Parameters:
        bucket (str): The name of the S3 bucket. Default is 'onesquared-databento'.
        region (str): The AWS region where the bucket is located. Default is 'us-east-1'.
        public_key (str): AWS access key ID. It can be provided as an argument or read from environment variables.
        private_key (str): AWS secret access key. It can be provided as an argument or read from environment variables.

    Returns:
        list: A list of file keys present in the specified S3 bucket.
    """

    # Log the function call with provided arguments
    logger.debug(f"get_s3_bucket_files called with bucket={bucket}, region={region}")

    # Create an S3 resource object using the provided credentials and region.
    s3 = boto3.resource(
        service_name="s3",
        region_name=region,
        aws_access_key_id=public_key,
        aws_secret_access_key=private_key,
    )

    # Get the S3 bucket object.
    bucket_obj = s3.Bucket(bucket)

    # List all objects (files) in the bucket and store their keys in a list.
    files = [file.key for file in bucket_obj.objects.all()]

    # Log the number of files retrieved
    logger.debug(f"Number of files retrieved: {len(files)}")

    # Return the list of file keys.
    return files


def get_s3_buckets(
    region: str = "us-east-1",
    public_key: str = os.environ.get("s3_public_key"),
    private_key: str = os.environ.get("s3_private_key"),
):
    """
    Get a list of AWS S3 bucket names available in the specified AWS region.

    Parameters:
        region (str): The AWS region to list the S3 buckets from. Default is 'us-east-1'.
        public_key (str): AWS access key ID. It can be provided as an argument or read from environment variables.
        private_key (str): AWS secret access key. It can be provided as an argument or read from environment variables.

    Returns:
        list: A list of AWS S3 bucket names available in the specified region.
    """

    # Log the function call with provided arguments
    logger.debug(f"get_s3_buckets called with region={region}")

    # Create an S3 resource object using the provided credentials and region.
    s3 = boto3.resource(
        service_name="s3",
        region_name=region,
        aws_access_key_id=public_key,
        aws_secret_access_key=private_key,
    )

    # List all buckets in the specified region and store their names in a list.
    buckets = [bucket.name for bucket in s3.buckets.all()]

    # Log the number of buckets retrieved
    logger.debug(f"Number of buckets retrieved: {len(buckets)}")

    # Return the list of bucket names.
    return buckets
