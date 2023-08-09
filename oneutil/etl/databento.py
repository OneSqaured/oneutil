import os
from datetime import date
import databento
import pandas as pd

from oneutil.etl.util import extract_date_from_string
from oneutil.logging import logger
from oneutil.etl.aws import (
    read_s3_bucket_file,
    get_files_in_s3_path,
)


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
    dbn = databento.DBNStore.from_bytes(body)

    # Convert the Databento store to a DataFrame
    df = dbn.to_df()

    # Return the DataFrame
    return df


def read_databento_from_s3(
    path: str,
    sd: date,
    ed: date,
    bucket: str = "onesquared-databento",
    region: str = "us-east-1",
    public_key: str = os.environ.get("s3_public_key"),
    private_key: str = os.environ.get("s3_private_key"),
):
    """
    Reads a sequence of files from an S3 folder between given start date (sd) and end date (ed).
    Combines the files into a DataFrame and returns it.

    Parameters:
        folder (str): The name of the S3 folder.
        sd (datetime.date): The start date.
        ed (datetime.date): The end date.

    Returns:
        pandas.DataFrame: The DataFrame containing the combined data from the specified files.

    Raises:
        FileNotFoundError: If any of the specified files are not found in the S3 bucket.
        Exception: If there are any errors during the conversion to DataFrame.
    """

    logger.debug(f"read_databento_from_s3 called with path={path}, sd={sd}, ed={ed}")

    # Get a list of all the files in the S3 folder
    files_in_folder = get_files_in_s3_path(
        path, bucket, region, public_key, private_key
    )

    # Filter files based on date range (sd and ed)
    # for file in files_in_folder:
    #     print(extract_date_from_filename(file))
    files_to_read = []
    for filename in files_in_folder:
        file_date = extract_date_from_string(filename)
        if file_date is not None and (sd <= file_date <= ed):
            files_to_read.append(filename)
    if not files_to_read:
        raise FileNotFoundError(f"No files found in the date range {sd} to {ed}")

    # Read data from the selected files and combine them into a DataFrame
    dfs_to_concat = []
    for filename in files_to_read:
        df = get_df_from_s3(filename, bucket, region, public_key, private_key)
        dfs_to_concat.append(df)

    combined_df = pd.concat(dfs_to_concat, ignore_index=True)

    return combined_df
