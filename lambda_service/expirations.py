import json
import logging
import time
import os

import botocore
import boto3
from boto3.dynamodb.conditions import Attr

from lambda_service.helpers import s3_remove_base_filename
from lambda_service.helpers import get_data_type_from_filename
from lambda_service.helpers import get_site_from_filename
from lambda_service.helpers import get_base_filename_from_full_filename
from lambda_service.helpers import validate_filename
from lambda_service.db import db_remove_base_filename

EXPIRATION_TABLE_NAME = os.getenv('EXPIRATION_TABLE', 'data-expiration-tracker')
dynamodb = boto3.resource('dynamodb')
expiration_table= dynamodb.Table(EXPIRATION_TABLE_NAME)


def data_type_has_expiration(data_type: str) -> bool:
    """ Define whether data should expire based on its data_type.

    The data_type can be found in the full filename preceeding the reduction level, near the end of the 
    For example, the filename wmd-ea03-20190621-00000007-EX01.fits.bz2 has data_type 'EX'.
    """
    expiration_data_types = ['EP', 'EF']  # experimental (EP) and focus (EF) data should expire. 
    return data_type in expiration_data_types


def get_image_lifespan(full_filename):
    """ Return the number of seconds an image should exist before automatic removal.

    Args: 
        full_filename (str): Image filename including the data_type, reduction_level, and extensions.
                             Example: wmd-ea03-20190621-00000007-EX01.fits.bz2
                             This is used to select the appropriate image lifespan.

    Returns:
        int: number of seconds before the image can be removed. 
    """

    if not validate_filename(full_filename):
        raise ValueError('The provided full filename input did not pass validation.')

    site = get_site_from_filename(full_filename)
    data_type = get_data_type_from_filename(full_filename)

    # Images from the test site (tst) should use 5 minute expirations for quicker debugging
    if site == 'tst':
        if data_type in ['EP', 'EF']:  # images with these data_types should expire
            return 300  # five minutes
        
    # For all regular (non-test-site) data:
    if data_type == "EP":
        return 86400 * 7  # Seven days for EP (experimental) data

    elif data_type == "EF":
        return 86400  # One day for EF (focus) data


def add_expiration_entry(base_filename, time_to_live_s):
    """ Add an entry to the expiration table.

    This table is responsible for tracking the data that will expire, and 
    triggering the removal functions at the right time. 
    
    Does nothing if the entry already exists. Include a 7 day expiration 
    time, after which the entry is automatically removed. 

    Args: 
        base_filename(str): wmd-ea03-20190621-00000007
        time_to_live_s (int): number of seconds before dynamodb can begin the deletion process

    """

    entry = {
        'pk': base_filename,
        'expiration_timestamp_s': int(time.time() + time_to_live_s)
    }

    # Exception for test images: expiration is 5 minutes after upload
    if base_filename[0:3] == "tst":
        entry['expiration_timestamp_s'] = int(time.time() + 300)

    try:
        dynamodb_response = expiration_table.put_item(
            Item=entry,
            ConditionExpression=Attr("pk").not_exists()
        )
    except botocore.exceptions.ClientError as e:
        # Ignore the ConditionalCheckFailedException, bubble up
        # other exceptions.
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise


def remove_expired_data_handler(event, context):
    records_to_delete = event['Records']
    for record in records_to_delete:

        if record["eventName"] != "REMOVE": 
            continue

        base_filename = record["dynamodb"]["Keys"]["pk"]["S"]

        s3_remove_base_filename(base_filename)
        db_remove_base_filename(base_filename)
    