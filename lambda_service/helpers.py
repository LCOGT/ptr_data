import boto3
import os
import re
import json
from astropy.io import fits
from botocore.client import Config

BUCKET_NAME = os.environ['BUCKET_NAME']
REGION = os.environ['REGION']

ssm_c = boto3.client('ssm', region_name=REGION)
s3_c = boto3.client('s3', region_name=REGION)

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


def get_s3_image_path(s3_directory, base_filename, data_type, reduction_level, file_type):
    full_filename = f"{base_filename}-{data_type}{reduction_level}.{file_type}"
    path = f"{s3_directory}/{full_filename}"
    return path


def get_s3_file_url(path, ttl=604800):
    s3 = boto3.client('s3', REGION, config=Config(signature_version='s3v4'))
    url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": BUCKET_NAME, "Key": path},
        ExpiresIn=ttl
    )
    return url


def read_s3_body(bucket_name, object_name):
    s3_object = s3_c.get_object(Bucket=bucket_name, Key=object_name)
    body = s3_object['Body']
    return body.read()


def scan_header_file(bucket, path):
    """
    Create a python dict from a fits-header-formatted text file in s3.

    :param bucket: String name of s3 bucket.
    :param path: String path to a text file in the bucket. 
        note - this file is assumed to be formatted as a fits header.
    :return: dictionary representation the fits header txt file.
    """
    data_entry = {}
    fits_line_length = 80

    contents = read_s3_body(bucket, path)
    
    for i in range(0, len(contents), fits_line_length):
        single_header_line = contents[i:i+fits_line_length].decode('utf8')


        # Split header lines according to the FITS header format
        values = re.split('=|/|',single_header_line)
        
        # Get the attribute and value for each line in the header.
        try:

            # The first 8 characters contains the attribute.
            attribute = single_header_line[:8].strip()
            # The rest of the characters contain the value 
            # (and sometimes a comment).
            after_attr = single_header_line[10:-1]

            # If the value (and not in a comment) is a string, then 
            # we want the contents inside the single-quotes.
            if "'" in after_attr.split('/')[0]:
                value = after_attr.split("'")[1].strip()
            
            # Otherwise, get the string preceding a comment 
            # (comments start with '/')
            else: 
                value = after_attr.split("/")[0].strip()

            # Add the attribute/value to a dict
            data_entry[attribute] = value

            if attribute == 'END': break
        
        except Exception as e:
            print(f"Error with parsing fits header: {e}")

    # Add the JSON representation of the data_entry to itself as the 
    # header attribute
    data_entry['JSON'] = json.dumps(data_entry)
    return data_entry


def get_header_from_fits(bucket, key):
    file_url = s3_c.generate_presigned_url('get_object', Params={"Bucket": bucket, "Key": key})
    fits_file = fits.open(file_url)
    header = dict(fits_file[0].header)
    header['JSON'] = json.dumps(header)
    return header



def parse_file_key(file_key):
    filename_no_extension, filename_extension = file_key.split('.', 1)  # only split on the first dot.
    filename_extension = filename_extension.split('.')[0]
    site, instrument, file_date, file_counter, data_type_level = filename_no_extension.split('-')

    #data_type = data_type_level[0:2]
    #reduction_level = data_type_level[2:4]
    data_type = get_data_type_from_filename(file_key)
    reduction_level = get_reduction_level_from_filename(file_key)

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

    # check that the base filename has four sections separated by '-'
    assert len(parts) == 4

    # check that the file starts with 3-letter site code 
    # like 'wmd'
    site = parts[0] 
    assert len(site) >= 3 and len(site) <=6

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


    # This expression gets the first string of alpha chars and the next group of numeric chars.
    obstype_reduction_level_re = re.compile("([a-zA-Z]+)([0-9]+)")
    observation_type, reduction_level = obstype_reduction_level_re.match(extension_parts[0]).groups()

    # check for the data type (usually ex (exposure), sometimes ep (experiemental))
    # example value: 'EX'
    #data_type = extension_parts[0][0:2] 
    assert observation_type and observation_type.isalpha()

    # check for the reduction_level, which directly follows the data_type.
    # example value: '01'
    #reduction_level = extension_parts[0][2:4]
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
    #return full_filename.split('-')[4].split('.')[0][0:2]
    obstype_reductionlevel = full_filename.split('-')[4].split('.')[0]
    obstype_regex = re.compile("([a-zA-Z]+)([0-9]+)")
    observation_type, _ = obstype_regex.match(obstype_reductionlevel).groups()
    return observation_type

def get_reduction_level_from_filename(full_filename):
    assert validate_filename(full_filename)
    obstype_reductionlevel = full_filename.split('-')[4].split('.')[0]
    obstype_regex = re.compile("([a-zA-Z]+)([0-9]+)")
    _, reduction_level = obstype_regex.match(obstype_reductionlevel).groups()
    return reduction_level


def get_site_from_filename(full_filename):
    assert validate_filename(full_filename)
    return full_filename.split('-')[0]


def get_site_from_base_filename(base_filename):
    assert validate_base_filename(base_filename)
    return base_filename.split('-')[0]


def s3_remove_base_filename(base_filename, s3_directory='data'):
    """ Remove data matching the base_filename from s3.

    This typically includes the header .txt, jpgs, large and small .fits. 

    Args: 
        base_filename(str): specifies the files to delete from s3. 
            Example: wmd-ea03-20190621-00000007
        s3_directory (str): this is the s3 'folder' the file is stored in. 
    """

    # first ensure that the base filename is in a valid format. 
    # otherwise, bad filenames might match with data that shouldn't be deleted.
    if validate_base_filename(base_filename): 

        prefix_to_delete = f"{s3_directory}/{base_filename}"
        print("prefix to delete: ")
        print(prefix_to_delete)

        # delete everything in the s3 bucket with the given prefix
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(BUCKET_NAME)
        response = bucket.objects.filter(Prefix=f"{s3_directory}/{base_filename}").delete()
        print("delete response: ")
        print(response)

