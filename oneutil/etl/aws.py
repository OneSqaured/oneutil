import boto3
import os

from oneutil.logging import logger


def get_s3_object(
    region: str = "us-east-1",
    public_key: str = os.environ.get("s3_public_key"),
    private_key: str = os.environ.get("s3_private_key"),
):
    """
    Creates and returns an Amazon S3 resource object with the provided credentials and region.

    Parameters:
        region (str): The AWS region to connect to. Defaults to "us-east-1".
        public_key (str): The AWS access key ID for authenticating with S3. If not provided,
            it will be fetched from the environment variable "s3_public_key".
        private_key (str): The AWS secret access key for authenticating with S3. If not provided,
            it will be fetched from the environment variable "s3_private_key".

    Returns:
        boto3.resources.factory.s3.ServiceResource: An Amazon S3 resource object.

    Raises:
        botocore.exceptions.NoCredentialsError: If the provided credentials are invalid or missing.
    """

    # Create an S3 resource object using the provided credentials and region.
    s3 = boto3.resource(
        service_name="s3",
        region_name=region,
        aws_access_key_id=public_key,
        aws_secret_access_key=private_key,
    )

    logger.debug("Successfully created the Amazon S3 resource object.")

    return s3


def get_s3_buckets(
    region: str = "us-east-1",
    public_key: str = os.environ.get("s3_public_key"),
    private_key: str = os.environ.get("s3_private_key"),
):
    """
    Get a list of AWS S3 bucket names available in the specified AWS region.

    Parameters:
        region (str): The AWS region to list the S3 buckets from. Default is 'us-east-1'.
        public_key (str): The AWS access key ID for authenticating with S3. If not provided,
            it will be fetched from the environment variable "s3_public_key".
        private_key (str): The AWS secret access key for authenticating with S3. If not provided,
            it will be fetched from the environment variable "s3_private_key".

    Returns:
        list: A list of AWS S3 bucket names available in the specified region.
    """

    # Log the function call with provided arguments
    logger.debug(f"get_s3_buckets called with region={region}")

    # Create an S3 resource object using the provided credentials and region.
    s3 = get_s3_object(region, public_key, private_key)

    # List all buckets in the specified region and store their names in a list.
    buckets = [bucket.name for bucket in s3.buckets.all()]

    # Log the number of buckets retrieved
    logger.debug(f"Number of buckets retrieved: {len(buckets)}")

    # Return the list of bucket names.
    return buckets


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
        public_key (str): The AWS access key ID for authenticating with S3. If not provided,
            it will be fetched from the environment variable "s3_public_key".
        private_key (str): The AWS secret access key for authenticating with S3. If not provided,
            it will be fetched from the environment variable "s3_private_key".

    Returns:
        list: A list of file keys present in the specified S3 bucket.
    """

    # Log the function call with provided arguments
    logger.debug(f"get_s3_bucket_files called with bucket={bucket}, region={region}")

    # Create an S3 resource object using the provided credentials and region.
    s3 = get_s3_object(region, public_key, private_key)

    # Get the S3 bucket object.
    bucket_obj = s3.Bucket(bucket)

    # List all objects (files) in the bucket and store their keys in a list.
    files = [file.key for file in bucket_obj.objects.all()]

    # Log the number of files retrieved
    logger.debug(f"Number of files retrieved: {len(files)}")

    # Return the list of file keys.
    return files


def read_s3_bucket_file(
    filename: str,
    bucket: str = "onesquared-databento",
    region: str = "us-east-1",
    public_key: str = os.environ.get("s3_public_key"),
    private_key: str = os.environ.get("s3_private_key"),
):
    """
    Read the contents of a file from a specified AWS S3 bucket.

    Parameters:
        filename (str): The name of the file to read from the S3 bucket.
        bucket (str): The name of the S3 bucket. Default is 'onesquared-databento'.
        region (str): The AWS region where the bucket is located. Default is 'us-east-1'.
        public_key (str): The AWS access key ID for authenticating with S3. If not provided,
            it will be fetched from the environment variable "s3_public_key".
        private_key (str): The AWS secret access key for authenticating with S3. If not provided,
            it will be fetched from the environment variable "s3_private_key".

    Returns:
        bytes: The contents of the specified file as bytes.

    Raises:
        botocore.exceptions.NoCredentialsError: If the provided credentials are invalid or missing.
        botocore.exceptions.ClientError: If there's an error accessing the S3 bucket or file.
    """

    logger.debug(
        f"read_s3_bucket_file called with bucket={bucket}, region={region}, filename={filename}"
    )

    # Create an S3 resource object using the provided credentials and region.
    s3 = get_s3_object(region, public_key, private_key)

    # Get the S3 bucket object.
    bucket_obj = s3.Bucket(bucket)

    # Get the S3 file object
    file_obj = bucket_obj.Object(filename)

    # Extract file body
    body = file_obj.get()["Body"].read()

    # Return the list of file keys.
    return body


def get_files_in_s3_path(
    path: str,
    bucket: str = "onesquared-databento",
    region: str = "us-east-1",
    public_key: str = os.environ.get("s3_public_key"),
    private_key: str = os.environ.get("s3_private_key"),
):
    """
    Retrieves a list of file names present in a specific folder in an Amazon S3 bucket.

    This function connects to the specified Amazon S3 bucket using the provided credentials
    (public_key and private_key) and retrieves a list of file names in the specified folder.
    The function uses the AWS SDK for Python (Boto3) to interact with the S3 service.

    Parameters:
        path (str): The name of the folder for which to retrieve file names.
        bucket (str, optional): The name of the S3 bucket. Default is "onesquared-databento".
        region (str, optional): The AWS region where the S3 bucket is located. Default is "us-east-1".
        public_key (str, optional): The public access key for the AWS account. If not provided,
                                   it will be retrieved from the environment variable "s3_public_key".
        private_key (str, optional): The private access key for the AWS account. If not provided,
                                     it will be retrieved from the environment variable "s3_private_key".

    Returns:
        List[str]: A list of file names (including the folder path) present in the specified folder.
                   The list will be empty if no files are found in the folder.

    Example:
        >>> get_files_in_s3_path("my_data_folder")
        ['my_data_folder/data_file1.csv', 'my_data_folder/data_file2.csv']

        >>> get_files_in_s3_path("empty_folder")
        []
    """

    # file_list = get_s3_bucket_files(bucket, region, public_key, private_key)
    # # Filter the file list to include only files in the specified folder
    # files_in_folder = [
    #     filename for filename in file_list if filename.startswith(path)
    # ]

    # Create an S3 resource object using the provided credentials and region.
    s3 = get_s3_object(region, public_key, private_key)

    # Get the S3 bucket object.
    bucket_obj = s3.Bucket(bucket)
    files_in_folder = [
        object_summary.key
        for object_summary in bucket_obj.objects.filter(Prefix=path, Delimiter="/")
        if not object_summary.key.endswith("/")
    ]
    num_files = len(files_in_folder)
    logger.debug(f"{num_files} files retrieved")
    return files_in_folder
