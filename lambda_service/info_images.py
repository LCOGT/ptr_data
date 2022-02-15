import urllib.parse
import boto3 
import logging
import os
import time
from botocore.exceptions import ClientError

from lambda_service.helpers import validate_filename, parse_file_key
from lambda_service.helpers import scan_header_file

from lambda_service.datastreamer import send_to_datastream

dynamodb = boto3.resource("dynamodb", region_name=os.getenv('REGION'))
info_table = dynamodb.Table(os.getenv('INFO_IMAGES_TABLE'))
info_images_ttl_s = int(os.getenv('INFO_IMAGES_TTL_HOURS')) * 3600

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handle_info_image_created(event, context): 
    logger.info("event: ", event)

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    # Sample file_path is like: data/wmd-ea03-20190621-00000007-EX00.fits.bz2
    file_path = urllib.parse.unquote_plus(
            event['Records'][0]['s3']['object']['key'], 
            encoding='utf-8'
        )

    # assume base filename is {site}-{instrument}-{yyyymmdd}-[12345678].{jpg|fits.bz2}
    file_key = file_path.split('/', 1)[-1]
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

    # Query dynamodb to find out which channel this image is intended for
    site_metadata = get_site_metadata(site)
    channel_number = get_info_channel(base_filename, site_metadata)
    if channel_number is None:
        logger.exception(f"Could not find channel number for {base_filename} in {site_metadata}")
        return 
    logger.info(f"Channel number for {base_filename} is {channel_number}")
    info_image_pk = f"{site}#{channel_number}"

    # Delete the existing record if uses an older (different) base_filename
    try:
        delete_response = info_table.delete_item(
            Key={
                'pk': info_image_pk,
            },
            ConditionExpression="base_filename <> :bf",  # <> is the not-equal comparator
            ExpressionAttributeValues={
                ':bf': base_filename
            },
            ReturnValues="ALL_OLD"
        )
    except ClientError as e:  
        # the delete did not happen because the condition check failed
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:  # undexpected error
            raise 

    # Compute the type of file-existence key to update the database with
    # eg. we might set "fits_01_exits" = True. 
    # TODO: maybe make this a list of files that exist, and update photonranch-api or ptr_ui to use that instead.
    file_exists_key = ''
    if file_extension == "jpg" and reduction_level == "10":
        file_exists_key = "jpg_medium_exists"
    elif file_extension == "jpg" and reduction_level == "11":
        file_exists_key = "jpg_small_exists"
    elif file_extension == "fits":
        file_exists_key = f"fits_{reduction_level}_exists"
    else:
        file_exists_key = f"other_{file_extension}_{reduction_level}_exists"

    # define dynamodb entry expiration time (timestamp, seconds)
    expiration_timestamp = int(time.time()) + info_images_ttl_s # 2 days from the present
    
    # Update the info-images table with the new data
    # This will create a new record if one does not currently exist. 
    update_response = info_table.update_item(
        Key={ 'pk': info_image_pk },
        UpdateExpression=f"set \
            base_filename=:bf, \
            expiration_timestamp=:et, \
            data_type=:dt, \
            file_date=:fd, \
            {file_extension}_{reduction_level}_file_path=:filepath, \
            {file_exists_key}=:fileexists",
        ExpressionAttributeValues={
            ':bf': base_filename,
            ':et': expiration_timestamp,
            ':dt': data_type,
            ':fd': file_date,
            ':filepath': file_path,
            ':fileexists': True,
        },
        ReturnValues="ALL_OLD"
    )
    print(update_response)

    if file_extension == "txt":
        header = scan_header_file(os.getenv('BUCKET_NAME'), file_path)
        header.pop('JSON')  # this key is not used for info images, we can remove it. 
        update_info_image_header(info_image_pk, header)

    # After we update the database, notify subscribers.
    try:
        logger.info('sending to subscribers: ')

        websocket_payload = {
            "s3_directory": "info-images",
            "base_filename": base_filename,
            "site": site,
            "channel": channel_number,
        }
        send_to_datastream(site, websocket_payload)

    except Exception as e:
        logger.exception(f'failed to send to subscribers: {str(e)}')


def update_info_image_header(info_image_pk, header):
    update_response = info_table.update_item(
        Key={ 'pk': info_image_pk }, 
        UpdateExpression="set header=:h",
        ExpressionAttributeValues={':h': header},
        ReturnValues="ALL_OLD"
    )
    print('saved header to database')


def get_site_metadata(site):
    """ Get the item in the info-images table with pk {site}#metadata. 
    This object is used to check what channel is intended for a given image. 
    """
    try: 
        response = info_table.get_item(Key={'pk': f'{site}#metadata'})
    except ClientError as e:
        print(e.response(['Error']['Message']))
    else:
        return response['Item']

    
def get_info_channel(base_filename, site_metadata):
    """ For new files arriving in s3, determine which channel they should be saved under. 

    This information is saved in the info-images table, in the row with pk {site}#metadata.
    Check whether the attributes channel1, channel2, or channel3 contain the base filename in question.
    If so, then we know where to save the incoming file's metadata. 
    If not, return None and handle in the parent.

    Args:
        base_filename (str): the incoming file in search of a channel number
        site_metadata (dict): the {site}#metadata object in the ddb info-images table. 
            It will probably contain a key 'channel{n}' with a value matching the provided base_filename. 

    """
    for key, val in site_metadata.items():
        if val == base_filename:
            # get the number from the key; ie. '2' from 'channel2'.        
            if 'channel' in key and len(key) == 8:  # validate to confirm the 'channelX' format.     
                return int(key.split('channel')[1])




