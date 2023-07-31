import os
import databento
from oneutil.logging import logger
from oneutil.etl.aws import read_s3_bucket_file


def get_df_from_s3(
    filename: str,
    bucket: str = "onesquared-databento",
    region: str = "us-east-1",
    public_key: str = os.environ.get("s3_public_key"),
    private_key: str = os.environ.get("s3_private_key"),
):
    """
    Fetches data from an S3 bucket, converts it to a DataFrame, and returns the DataFrame.

    Parameters:
        filename (str): The name of the file to read from the S3 bucket.
        bucket (str): The name of the S3 bucket. Default is 'onesquared-databento'.
        region (str): The AWS region where the bucket is located. Default is 'us-east-1'.
        public_key (str): The AWS access key ID for authenticating with S3. If not provided,
            it will be fetched from the environment variable "s3_public_key".
        private_key (str): The AWS secret access key for authenticating with S3. If not provided,
            it will be fetched from the environment variable "s3_private_key".

    Returns:
        pandas.DataFrame: The DataFrame containing the data from the specified file.

    Raises:
        FileNotFoundError: If the specified file is not found in the S3 bucket.
        Exception: If there are any errors during the conversion to DataFrame.

    Note:
        The function assumes that the required AWS credentials (public_key and private_key)
        are available as environment variables: "s3_public_key" and "s3_private_key".

    Example:
        # Fetch data from the file "data.csv" in the S3 bucket "my-data-bucket"
        df = get_df_from_s3(filename="data.csv", bucket="my-data-bucket", region="us-west-2")
    """

    logger.debug(
        f"get_df_from_s3 called with filename={filename}, bucket={bucket}, region={region}"
    )

    # Read the file content from the S3 bucket using the provided filename
    body = read_s3_bucket_file(filename, bucket, region, public_key, private_key)

    # Create a Databento store from the bytes read from the S3 bucket
    dbn = databento.DNBStore.from_bytes(body)

    # Convert the Databento store to a DataFrame
    df = dbn.to_df()

    # Return the DataFrame
    return df
