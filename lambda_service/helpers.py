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

# Example filename: wmd-ea03-20190621-00000007-EX00.fits.bz2
def validate_filename(filename):

    parts = filename.split('-')

    # check that the file starts with 3-letter site code 
    # like 'wmd'
    site = parts[0] 
    assert len(site) == 3 and site.isalpha()

    # check for 4-letter instrument name, 
    # like 'ea03'
    inst = parts[1]  
    assert len(inst) == 4

    # check for date formatted as yyymmdd
    # like '20190621'
    date = parts[2]  
    assert len(date) == 8 and date.isdigit()

    # check for 8-digit image number
    # like '00000007'
    incr = parts[3]  
    assert len(incr) == 8 and incr.isdigit()

    extensions = parts[4]   
    extension_parts = extensions.split('.')

    # check for the EX** indicating filetype, where ** is a 2 digit number
    # like 'EX01'
    filetype = extension_parts[0]  
    assert len(filetype) == 4 and filetype[2:4].isnumeric

    # check for a file extension that is letters only 
    # like 'txt', 'fits', or 'jpg'
    file_extension = extension_parts[1]
    assert file_extension.isalpha()

    return True


def s3_remove_base_filename(base_filename):
    """ Remove data matching the base_filename from s3.

    This typically includes the header .txt, jpgs, large and small .fits. 

    Args: 
        base_filename(str): specifies the files to delete from s3. 
            Example: wmd-ea03-20190621-00000007
    """

    # first ensure that the base filename is in a valid format. 
    # otherwise, bad filenames might match with data that shouldn't be deleted.

    # make a full filename used for validation. 
    # the suffix doesn't matter as long as it's in the right the format for validation
    filename = f"{base_filename}-EP00.txt"  
    
    if validate_filename(filename): 

        prefix_to_delete = f"data/{base_filename}"

        # delete everything in the s3 bucket with the given prefix
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(BUCKET_NAME)
        bucket.objects.filter(Prefix=base_filename).delete()

