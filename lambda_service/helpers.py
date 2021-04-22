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

def get_s3_image_path(base_filename, data_type, reduction_level, file_type):
    full_filename = f"{base_filename}-{data_type}{reduction_level}.{file_type}"
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


def parse_file_key(file_key):
    filename_no_extension, filename_extension = file_key.split('.', 1)  # only split on the first dot.
    filename_extension = filename_extension.split('.')[0]
    site, instrument, file_date, file_counter, data_type_level = filename_no_extension.split('-')
    data_type = data_type_level[0:2]
    reduction_level = data_type_level[2:4]
    base_filename = '-'.join([site, instrument, file_date, file_counter])
    return {
        "base_filename": base_filename,
        "file_extension": filename_extension,
        "site": site,
        "instrument": instrument,
        "file_date": file_date,
        "file_counter": file_counter,
        "data_type": data_type,
        "reduction_level": reduction_level
    }


# TODO: It would be useful to refactor all filename helpers into a separate module. That would also provide a place
# for better filename documentation. 
# Would the filename be well suited for a simple class? Easy to get any single piece or str representation. 


# Example base filename: wmd-ea03-20190621-00000007
def validate_base_filename(filename):

    parts = filename.split('-')

    # check that the file starts with 3-letter site code 
    # like 'wmd'
    site = parts[0] 
    assert len(site) >= 3 and len(site) <=6

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

    # If no exceptions are raised, the base filename is valid
    return True



# Example filename: wmd-ea03-20190621-00000007-EX00.fits.bz2
def validate_filename(filename):

    parts = filename.split('-')

    base_filename = '-'.join(parts[:4])
    print(base_filename)
    assert validate_base_filename(base_filename)

    extensions = parts[4]   
    extension_parts = extensions.split('.')

    # check for the data type (usually ex (exposure), sometimes ep (experiemental))
    # example value: 'EX'
    data_type = extension_parts[0][0:2] 
    assert len(data_type) == 2 and data_type.isalpha()

    # check for the reduction_level, which directly follows the data_type.
    # example value: '01'
    reduction_level = extension_parts[0][2:4]
    assert len(reduction_level) == 2 and reduction_level.isdigit()

    # check for a file extension that is letters only 
    # like 'txt', 'fits', or 'jpg'
    file_extension = extension_parts[1]
    assert file_extension.isalpha()

    # If none of the assert statements raise an exception, then the filename is valid
    return True


def get_base_filename_from_full_filename(full_filename):
    """ Given a full filename, extract and return the base_filename substring. 
    
    Example full filename: tst-aa00-20201231-12345678-EX01.fits.bz2
    Corresponding base filename: tst-aa00-20201231-12345678
    """
    assert validate_filename(full_filename)
    return '-'.join(full_filename.split('-')[:4])

def get_data_type_from_filename(full_filename):
    assert validate_filename(full_filename) 
    return full_filename.split('-')[4].split('.')[0][0:2]


def get_site_from_filename(full_filename):
    assert validate_filename(full_filename)
    return full_filename.split('-')[0]


def get_site_from_base_filename(base_filename):
    assert validate_base_filename(base_filename)
    return base_filename.split('-')[0]


def s3_remove_base_filename(base_filename):
    """ Remove data matching the base_filename from s3.

    This typically includes the header .txt, jpgs, large and small .fits. 

    Args: 
        base_filename(str): specifies the files to delete from s3. 
            Example: wmd-ea03-20190621-00000007
    """

    # first ensure that the base filename is in a valid format. 
    # otherwise, bad filenames might match with data that shouldn't be deleted.
    if validate_base_filename(base_filename): 

        prefix_to_delete = f"data/{base_filename}"
        print("prefix to delete: ")
        print(prefix_to_delete)

        # delete everything in the s3 bucket with the given prefix
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(BUCKET_NAME)
        response = bucket.objects.filter(Prefix=base_filename).delete()
        print("delete response: ")
        print(response)

