import urllib.parse
import boto3
import os
import json

from lambda_service.handler import _remove_connection
from lambda_service.db import update_header_data, update_new_image, header_data_exists
from lambda_service.db import DB_ADDRESS

from lambda_service.helpers import validate_filename, parse_file_key
from lambda_service.helpers import scan_header_file, get_header_from_fits

from lambda_service.expirations import add_expiration_entry
from lambda_service.expirations import data_type_has_expiration
from lambda_service.expirations import get_image_lifespan

from lambda_service.thumbnails import resize_handler

from lambda_service.handler import sendToSubscribers
from lambda_service.datastreamer import send_to_datastream

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb", region_name=os.getenv('REGION'))
s3_c = boto3.client('s3', region_name=os.getenv('REGION'))

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
    
    # The file_key is the full filename that looks something like 
    # 'wmd-ea03-20190621-00000007-EX00.fits.bz2'
    file_key = file_path.split('/')[-1]

    try:
        validate_filename(file_key)
    except AssertionError:
        logger.exception(f"Invalid filename {file_key}; failed to update database")
        return
    
    # Extract the various pieces of information from the filename
    file_parts = parse_file_key(file_key)
    base_filename = file_parts["base_filename"]
    file_extension = file_parts["file_extension"]
    site = file_parts["site"]
    instrument = file_parts["instrument"]
    file_date = file_parts["file_date"]
    file_counter = file_parts["file_counter"]
    data_type = file_parts["data_type"]
    reduction_level = file_parts["reduction_level"]

    logger.info(f"Parsed filename: {file_parts}")

    # Set the TTL for images that should expire
    if data_type_has_expiration(data_type):
        time_until_expiration = get_image_lifespan(file_key)
        add_expiration_entry(base_filename, time_until_expiration)

    # If the new file is the header file (in txt format)
    if file_extension == 'txt':
        # Parse the header txt file
        header_data = scan_header_file(bucket, file_path)
        # Update the database
        update_header_data(DB_ADDRESS, base_filename, data_type, header_data)

    # If the new file is an image (jpg or fits)
    #elif file_extension in ['jpg', 'fits']:
    elif file_extension == 'fits':
        update_new_image(DB_ADDRESS, base_filename, data_type, reduction_level, file_extension)

        # Fallback on the fits file for the header data if the header has not yet been supplied
        if not header_data_exists(DB_ADDRESS, base_filename):
            header_data = get_header_from_fits(bucket, file_path)
            update_header_data(DB_ADDRESS, base_filename, data_type, header_data)

    elif file_extension == 'jpg':
        update_new_image(DB_ADDRESS, base_filename, data_type, reduction_level, file_extension)

        # generate thumbnail from the larger jpg
        if reduction_level == "10":
            thumbnail_key = f"{file_path.split('/')[0]}/{site}-{instrument}-{file_date}-{file_counter}-{data_type}11.jpg"
            thumbnail_height = 128  # height in pixels for the rescaling output
            try:
                resize_handler(bucket, file_path, thumbnail_key, thumbnail_height)
            except Exception as e:
                logger.exception(e)
                pass  


    # Unknown file extension:
    else: 
        logger.warn(f"Unrecognized file extension {file_extension}. Skipping file.")

    # After we update the database, notify subscribers.
    try:
        logger.info('sending to subscribers: ')

        # This is the old websocket method. Should be deprecated once the datastreamer
        # service is fully integrated.
        websocket_payload = {
            "s3_directory": "data",
            "base_filename": base_filename,
            "site": site
        }
        #sendToSubscribers(websocket_payload)

        # This is the new way to stream data. Remove the websocket from this repository
        # once this method is integrated across the PTR stack. 
        send_to_datastream(site, websocket_payload)

    except Exception as e:
        logger.exception(f'failed to send to subscribers: {str(e)}')
