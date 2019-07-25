# aws.py

import boto3
import psycopg2
from botocore.client import Config
import time, re, json

REGION = 'us-east-1'
URL_EXPIRATION = 3600 # Seconds until URL expiration

s3_c = boto3.client('s3', region_name=REGION)
s3_r = boto3.resource('s3', region_name=REGION)

# Code from https://alexwlchan.net/2018/01/listing-s3-keys-redux/
def get_matching_s3_objects(bucket, prefix='', suffix=''):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)

        try:
            contents = resp['Contents']
        except KeyError:
            return

        for obj in contents:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield obj

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

def read_s3_body(bucket_name, object_name):
    s3_object = s3_c.get_object(Bucket=bucket_name, Key=object_name)
    body = s3_object['Body']

    return body.read()

def scan_s3_image_data(bucket, file_prefix='', file_suffix=''):
    print('\nSEARCHING FOR IMAGE META-DATA STORED IN BUCKET: %s' % bucket)

    # Parse through every matching text file returned from an S3 
    # Each file corresponds to a dict stored in the returned list
    data = []
    fits_line_length = 80
    for key in get_matching_s3_objects(bucket, prefix=file_prefix, suffix=file_suffix): 
        path = key['Key']
        contents = read_s3_body(bucket, path)

        data_entry = {}
        print('SCANNING: ' + path)
        for i in range(0, len(contents), fits_line_length):
            single_header_line = contents[i:i+fits_line_length].decode('utf8')

            # Split line twice according to '=' and '/' characters
            values = re.split('=|/|',single_header_line)
            
            try:
                # Remove extra characters. Attribute values are first
                # stripped of single quotation marks before whitespace is removed.   
                attribute = values[0].strip()
                attribute_value = values[1].replace("'", "").strip() 
                
                data_entry[attribute] = attribute_value
            except:
                if attribute == 'END':
                    break

        # Add the JSON representation of the data_entry to itself      
        data_entry['JSON'] = json.dumps(data_entry)
        data.append(data_entry)

    return data