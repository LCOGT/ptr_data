import boto3
import os
from botocore.client import Config

BUCKET_NAME = os.environ['BUCKET_NAME']
REGION = os.environ['REGION']

ssm_c = boto3.client('ssm', region_name=REGION)

def get_secret(key):
    """
    Some parameters are stored in AWS Systems Manager Parameter Store.
    This replaces the .env variables we used to use with flask.
    """
    resp = ssm_c.get_parameter(
    	Name=key,
    	WithDecryption=True
    )
    return resp['Parameter']['Value']

def get_s3_image_path(base_filename, ex_value, file_extension):
    full_filename = f"{base_filename}-{ex_value}.{file_extension}"
    path = f"data/{full_filename}"
    return path

def get_s3_file_url(path, ttl=604800):
    s3 = boto3.client('s3', REGION, config=Config(signature_version='s3v4'))
    url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": BUCKET_NAME, "Key": path},
        ExpiresIn=ttl
    )
    return url
