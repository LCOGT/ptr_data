import json
import logging
import time
import os

import botocore
import boto3
from boto3.dynamodb.conditions import Attr

from lambda_service.helpers import s3_remove_base_filename
from lambda_service.db import db_remove_base_filename

dynamodb = boto3.resource('dynamodb')
EXPIRATION_TABLE_NAME = os.getenv('EXPIRATION_TABLE', 'data-expiration-tracker')
EXPIRATION_TABLE = dynamodb.Table(EXPIRATION_TABLE_NAME)


def add_expiration_entry(base_filename, time_to_live_s):
    """ Add an entry to the expiration table.

    This table is responsible for tracking the data that will expire, and 
    triggering the removal functions at the right time. 
    
    Does nothing if the entry already exists. Also include a 7 day expiration 
    time, after which the entry is automatically removed. 

    Args: 
        base_filename(str): wmd-ea03-20190621-00000007

    """

    entry = {
        'pk': base_filename,
        'expiration_timestamp_s': int(time.time() + time_to_live_s)
    }

    try:
        EXPIRATION_TABLE.put_item(
            Item=entry,
            ConditionExpression=Attr("pk").not_exists()
        )
    except botocore.exceptions.ClientError as e:
        # Ignore the ConditionalCheckFailedException, bubble up
        # other exceptions.
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise


def remove_expired_data_handler(event, context):
    print(event)
    records_to_delete = event['Records']
    for record in records_to_delete:

        if record["event_name"] != "DELETE": 
            continue

        base_filename = record["dynamodb"]["Keys"]["pk"]

        s3_remove_base_filename(base_filename)
        db_remove_base_filename(base_filename)
    