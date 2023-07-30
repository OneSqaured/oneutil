# One Squared Utilities

oneutil is a Python package that provides useful utilities for work we do at One Squared.

## Installation

To install oneutil, you can get it from PYPI:

```bash
pip install oneutil

```

For testing, you can install it locally via

```bash
python setup.py sdist bdist_wheel
pip install -e . 
```

## Usage

```python
from oneutil.etl.aws import get_s3_bucket_files, get_s3_buckets

# List files in the default S3 bucket in 'us-east-1' region
files = get_s3_bucket_files()

# List all available S3 buckets in 'us-east-1' region
buckets = get_s3_buckets()
```

oneutil relies on the AWS SDK (boto3) for interacting with AWS S3. To use the package, make sure you have valid AWS access credentials (access key ID and secret access key) set as environment variables or provide them explicitly when calling the functions.

- `s3_public_key`: The AWS access key ID.
- `s3_private_key`: The AWS secret access key.

If the environment variables are not set, you can pass the credentials directly as arguments to the functions.
