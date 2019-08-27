import json
import urllib.parse
import boto3
import psycopg2
import re
import os

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_c = boto3.client('s3', region_name='us-east-1')

def main(event, context):
    
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    # Sample file_path is like: WMD/raw_data/2019/WMD-ea03-20190621-00000007-E00.fits.bz2
    file_path = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    # Format amazon upload timestamp (yyyy-mm-ddThh:mm:ss.mmmZ) 
    # to psql timestamp without timezone (yyyy-mm-dd hh:mm:ss).
    last_modified = list(urllib.parse.unquote_plus(event['Records'][0]['eventTime']))
    last_modified[10] = ' '
    last_modified = last_modified[:-5]
    last_modified = "".join(last_modified)
    
    # The 'filename' that looks somethign like 'WMD-ea03-20190621-00000007-E00.fits.bz2'
    file_key = file_path.split('/')[-1]
    
    # The base_filename (aka primary key) is something like 'WMD-ea03-20190621-00000007'
    base_filename = file_key[:26]
    
    # The data_type is the 'E00' string after the base_filename.
    data_type = file_key[27:30]
    
    # The file_extension signifies the filetype, such as 'fits' or 'txt'.
    file_extension = file_key.split('.')[1]
    
    # The site is derived from the beginning of the base filename (eg. 'WMD')
    site = base_filename[0:3] 
    
    print(f"file_path: {file_path}")
    print(f"base filename: {base_filename}")
    print(f"data_type: {data_type}")
    print(f"file_extension: {file_extension}")
    print(f"site: {site}")

    # Set user_id
    user_id = 171
  
    if file_extension == "txt":
            
        header_data = scan_header_file(bucket, file_path)
        
        sql = ("INSERT INTO images("

               "base_filename, "
               "created_user, "
               "site, "
               "capture_date, "
               "sort_date, "
               "right_ascension, "
               "declination, "
               "altitude, "
               "azimuth, "
               "filter_used, "
               "airmass, "
               "exposure_time, "
               "header) "

               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
               "ON CONFLICT (base_filename) DO UPDATE SET "

               "created_user = excluded.created_user, "
               "capture_date = excluded.capture_date, "
               "sort_date = excluded.sort_date, "
               "right_ascension = excluded.right_ascension, "
               "declination = excluded.declination, "
               "altitude = excluded.altitude, "
               "azimuth = excluded.azimuth, "
               "filter_used = excluded.filter_used, "
               "airmass = excluded.airmass, "
               "exposure_time = excluded.exposure_time, "
               "header = excluded.header;"
        )

        # extract values from header data
        capture_date = header_data.get('DATE-OBS')
        header = header_data.get('JSON')
        right_ascension = header_data.get('MNT-RA')
        declination = header_data.get('MNT-DEC')
        altitude = header_data.get('ALTITUDE')
        azimuth = header_data.get('AZIMUTH')
        filter_used = header_data.get('FILTER')
        airmass = header_data.get('AIRMASS')
        exposure_time = header_data.get('EXPTIME')
        
        try: 
            capture_date = re.sub('T', ' ', capture_date) # format capture time as SQL timestamp
        except: 
            capture_date = last_modified
        
        # These values will be fed into the sql command string (above)
        attribute_values = [
            base_filename,
            user_id,
            site,
            capture_date,
            capture_date, # capture_date is also used for the 'sort_date' attribute.
            right_ascension,
            declination,
            altitude,
            azimuth,
            filter_used,
            airmass,
            exposure_time,
            header
        ]

        valid_sql_to_execute = True
        
    # Handle non-text files (eg. fits or jpg)
    elif file_extension in ['fits', 'jpg']:
        
        # Define the attribue column we will set to true.
        file_exists_attribute = f"{data_type.lower()}_{file_extension}_exists"

        # Create a new element with the primary key, site, and file_exists_attribue=true. 
        # If there is already an element with this primary key, update the state of file_exists_attribute = true
        # TODO: rewrite to avoid injection vulnerability. Risk is lower because site code automatically controls the filenames, but still not good.
    
        sql = (f"INSERT INTO images (base_filename, site, sort_date, {file_exists_attribute}) "
                "VALUES(%s, %s, %s, %s) ON CONFLICT (base_filename) DO UPDATE "
                f"SET {file_exists_attribute} = excluded.{file_exists_attribute};"
        )
        
        # These values will be fed into the sql command string (above)
        attribute_values = (
            base_filename, 
            site, 
            last_modified, 
            True
        )

        valid_sql_to_execute = True

    else:
        print(f"Unrecognized file type: {file_extension}. Skipping file.")
        items_not_added += 1
    
   
    connection = None
    try:
        
        database = os.environ['DB_DATABASE']
        user = os.environ['DB_USER']
        password = os.environ['DB_PASSWORD']
        host = os.environ['DB_HOST']
        port = os.environ['DB_PORT']
        
        # Connect to database
        print('Establishing RDS connection...')
        connection = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        cursor = connection.cursor()
        print('Connection established.')

        if valid_sql_to_execute:
            cursor.execute(sql,attribute_values)

        connection.commit()
        print('Successfully updated database.')
    except (Exception, psycopg2.Error) as error :
        print('Failed to update database.')
        print(error)
    finally:
        #closing database connection.
        if(connection):
            cursor.close()
            connection.close()

    return True


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
            # The rest of the characters contain the value (and sometimes a comment).
            after_attr = single_header_line[10:-1]

            # If the value (and not in a comment) is a string, then 
            # we want the contents inside the single-quotes.
            if "'" in after_attr.split('/')[0]:
                value = after_attr.split("'")[1].strip()
            
            # Otherwise, get the string preceding a comment (comments start with '/')
            else: 
                value = after_attr.split("/")[0].strip()

            # Add the attribute/value to a dict
            data_entry[attribute] = value

            if attribute == 'END': break
        
        except Exception as e:
            print(f"Error with parsing fits header: {e}")

    # Add the JSON representation of the data_entry to itself as the header attribute
    data_entry['JSON'] = json.dumps(data_entry)

    return data_entry

def read_s3_body(bucket_name, object_name):
    s3_object = s3_c.get_object(Bucket=bucket_name, Key=object_name)
    body = s3_object['Body']
    return body.read()

