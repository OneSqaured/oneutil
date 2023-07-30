import boto3 as boto3

# Authenticate
s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-1',
    aws_access_key_id='',
    aws_secret_access_key=''
)

# Print out bucket names
for bucket in s3.buckets.all():
    print("Bucket Name: "+bucket.name)
    # Print out file names
    for obj in s3.Bucket(bucket.name).objects.all():
        print(obj.key)



