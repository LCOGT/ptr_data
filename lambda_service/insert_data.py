import json
import urllib.parse
import boto3
import psycopg2
import re
import os

from lambda_service.handler import _remove_connection
from lambda_service.db import update_header_data, update_new_image
from lambda_service.db import DB_ADDRESS
from lambda_service.helpers import validate_filename

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
s3_c = boto3.client('s3', region_name='us-east-1')

SUBSCRIBERS_TABLE = os.getenv('SUBSCRIBERS_TABLE')


def _send_to_connection(gatewayClient, connection_id, data, wss_url):
    return gatewayClient.post_to_connection(
        ConnectionId=connection_id,
        Data=json.dumps({"messages":[{"content": data}]}).encode('utf-8')
    )


def sendToSubscribers(data):
    wss_url = os.getenv('WSS_URL')
    gatewayApi = boto3.client("apigatewaymanagementapi", endpoint_url=wss_url)

    # Get all current connections
    table = dynamodb.Table(SUBSCRIBERS_TABLE)
    response = table.scan(ProjectionExpression="ConnectionID")
    items = response.get("Items", [])
    connections = [x["ConnectionID"] for x in items if "ConnectionID" in x]

    # Send the message data to all connections
    logger.debug("Broadcasting message: {}".format(data))
    dataToSend = {"messages": [data]}
    for connectionID in connections:
        try: 
            connectionResponse = _send_to_connection(
                    gatewayApi, connectionID, dataToSend, os.getenv('WSS_URL'))
            logger.info((
                f"connection response: "
                f"{json.dumps(connectionResponse)}")
            )
        except gatewayApi.exceptions.GoneException:
            error_msg = (
                f"Failed sending to {connectionID} due to GoneException. "
                "Removing it from the connections table."
            )
            logger.exception(error_msg)
            _remove_connection(connectionID)
            continue


def handle_s3_object_created(event, context):

    logger.info("event: ", event)
    
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    # Sample file_path is like: data/wmd-ea03-20190621-00000007-EX00.fits.bz2
    file_path = urllib.parse.unquote_plus(
            event['Records'][0]['s3']['object']['key'], 
            encoding='utf-8'
        )
    
    # Format amazon upload timestamp (yyyy-mm-ddThh:mm:ss.mmmZ) 
    # to psql timestamp without timezone (yyyy-mm-dd hh:mm:ss).
    last_modified = list(urllib.parse.unquote_plus(
            event['Records'][0]['eventTime']))
    last_modified[10] = ' '
    last_modified = last_modified[:-5]
    last_modified = "".join(last_modified)
    
    # The 'filename' that looks something like 
    # 'wmd-ea03-20190621-00000007-EX00.fits.bz2'
    file_key = file_path.split('/')[-1]

    try:
        validate_filename(file_key)
    except AssertionError:
        logger.exception(f"Unexpected filename {file_key}; failed to update \
            database")
        return
    
    # The base_filename (aka primary key) is something like 
    # 'wmd-ea03-20190621-00000007'
    base_filename = file_key[:26]
    
    # The data_type is the 'EX00' string after the base_filename.
    data_type = file_key[27:31]
    
    # The file_extension signifies the filetype, such as 'fits' or 'txt'.
    file_extension = file_key.split('.')[1]
    
    # The site is derived from the beginning of the base filename (eg. 'wmd')
    site = base_filename[0:3] 
    
    logger.info("Parsed filename: ",{
        "file_path": file_path,
        "base filename": base_filename,
        "data_type": data_type,
        "file_extension": file_extension,
        "site": site,
    })

    # If the new file is the header file (in txt format)
    if file_extension == 'txt':
        # Parse the header txt file
        header_data = scan_header_file(bucket, file_path)
        # Update the database
        update_header_data(DB_ADDRESS, base_filename, header_data)
    # If the new file is an image (jpg or fits)
    elif file_extension in ['jpg', 'fits']:
        update_new_image(DB_ADDRESS, base_filename, data_type, file_extension)
    # Unknown file type:
    else: 
        logger.warn(f"Unrecognized file type {file_extension}. Skipping file.")

    # After we update the database, notify subscribers.
    try:
        logger.info('sending to subscribers: ')
        sendToSubscribers(base_filename)
    except Exception as e:
        logger.exception(f'failed to send to subscribers: {str(e)}')


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
            logger.exception(f"Error with parsing fits header: {e}")

    # Add the JSON representation of the data_entry to itself as the 
    # header attribute
    data_entry['JSON'] = json.dumps(data_entry)
    return data_entry


def read_s3_body(bucket_name, object_name):
    s3_object = s3_c.get_object(Bucket=bucket_name, Key=object_name)
    body = s3_object['Body']
    return body.read()
