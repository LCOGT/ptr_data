import os
import boto3
import json
import requests
import time

from lambda_service.helpers import timestamp_to_isodate_utc
from lambda_service.helpers import filesize_readable
from lambda_service.helpers import get_site_from_filename

dynamodb = boto3.resource("dynamodb", region_name=os.getenv('REGION'))
recent_uploads_table = dynamodb.Table(os.getenv('UPLOADS_LOG_TABLE'))

# Expire s3 log entries after this amount of time
log_entry_ttl_s = int(os.getenv('UPLOADS_LOG_TTL_HOURS', 48)) * 3600

def log_new_upload(filename: str, upload_timestamp_s: int, size_bytes: int):
    """ Handler function that calls all the subroutines involved logging new s3 uploads.
    That includes saving some info in a dynamodb table and streaming a notice to the userstatus panel in the UI. 

    Args:
        filename (str): full filename for the new object in s3. Eg: data/wmd-ea03-20190621-00000007-EX00.fits.bz2
        upload_timestamp_s (int): unix timestamp in seconds when the file was recieved in s3
        size_bytes (int): filesize for the uploaded object
    """

    # filename will be full object key like
    # data/wmd-ea03-20190621-00000007-EX00.fits.bz2
    site = get_site_from_filename(filename.split('/')[-1])

    add_to_recent_uploads_log(filename, upload_timestamp_s, size_bytes, site)

    if site != 'unknown_site':
        pass
        # Dropping this functionality for now as it clutters the log. 
        #send_new_upload_to_site_activity_log(filename, upload_timestamp_s, size_bytes, site)


def add_to_recent_uploads_log(filename: str, upload_timestamp_s: int, size_bytes: int, site: str):
    """ Add the file to the s3 uploads log dynamodb table. Entries are set to expire in 2 days.

    Args:
        filename (str): full filename for the new object in s3. Eg: data/wmd-ea03-20190621-00000007-EX00.fits.bz2
        upload_timestamp_s (int): unix timestamp in seconds when the file was recieved in s3
        size_bytes (int): filesize for the uploaded object
        site (str): site associated with the file

    Returns:
        dynamodb table put_item response
    """

    log_entry = {
        "site": site,
        "upload_timestamp_s": int(upload_timestamp_s),
        "expire_timestamp_s": int(upload_timestamp_s + log_entry_ttl_s),
        "filename": filename,
        "size_bytes": int(size_bytes),
    }
    response = recent_uploads_table.put_item( Item=log_entry )
    return response


def send_new_upload_to_site_activity_log(filename: str, upload_timestamp_s: int, size_bytes: int, site: str): # aka userstatus
    """ Send notice of the new file upload to the userstatus logs in the UI.

    Args:
        filename (str): full filename for the new object in s3. Eg: data/wmd-ea03-20190621-00000007-EX00.fits.bz2
        upload_timestamp_s (int): unix timestamp in seconds when the file was recieved in s3
        size_bytes (int): filesize for the uploaded object
        site (str): site associated with the file
    """

    # filename will be full object key like data/wmd-ea03-20190621-00000007-EX00.fits.bz2
    # We don't want the 'data/' prefix, just the filename.
    filename_no_prefix = filename.split('/')[-1]
    readable_time = timestamp_to_isodate_utc(upload_timestamp_s)[:-8] + 'Z'  # remove decimals in timestamp
    readable_filesize = filesize_readable(size_bytes)
    
    log_message = f"new in s3: {filename_no_prefix} | {readable_filesize} | {readable_time} "

    url = "https://logs.photonranch.org/logs/newlog"
    body = json.dumps({
        "site": site,
        "log_message": log_message,
        "log_level": "info",
        "timestamp": time.time(),
    })
    resp = requests.post(url, body)
    print(resp)
