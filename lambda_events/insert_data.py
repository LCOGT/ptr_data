import json
import urllib.parse
import boto3
import psycopg2
import re

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    
    # Get the object from the event and show its content type
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    
    # Sample file_path is like: WMD/raw_data/2019/WMD-ea03-20190621-00000007-E00.fits.bz2
    file_path = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    # Format amazon upload timestamp (yyyy-mm-ddThh:mm:ss.mmmZ) 
    # to psql timestamp without timezone (yyyy-mm-dd hh:mm:ss).
    upload_time = list(urllib.parse.unquote_plus(event['Records'][0]['eventTime']))
    upload_time[10] = ' '
    upload_time = upload_time[:-5]
    upload_time = "".join(upload_time)
    
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
    
    print(f"upload time: {upload_time}")
    print(f"base filename: {base_filename}")
    print(f"data_type: {data_type}")
    print(f"file_extension: {file_extension}")
    print(f"site: {site}")
    
    
    
    # Handle text file (which stores the fits header data)
    if file_extension == "txt":
        
        header_data = parse_file(bucket_name, file_path)
        
        sql = "INSERT INTO images(image_root, observer, site, capture_date, sort_date, right_ascension, header) VALUES (%s, %s, %s, %s, %s, %s)"

        # extract values from header data
        #image_root = header_data['FILENAME']
        observer = header_data['OBSERVER']
        site = header_data['FILENAME']
        capture_date = header_data['DATE-OBS']
        header = header_data['JSON']
        right_ascension = header_data['MNT-RA']
        
        capture_date = re.sub('T', ' ', capture_date) # format capture time as SQL timestamp
        
        # format row for SQL insertion
        attribute_values = [
            base_filename, # previously named image_root
            observer,
            site,
            capture_date,
            capture_date,
            right_ascension,
            header
        ]

    # Handle non-text files (eg. fits or jpg)
    else:
        
        # Define the attribue column we will set to true.
        file_exists_attribute = f"{data_type.lower()}_{file_extension}_exists"

        # Create a new element with the primary key, site, and file_exists_attribue=true. 
        # If there is already an element with this primary key, update the state of file_exists_attribute = true
        # TODO: rewrite to avoid injection vulnerability. Risk is lower because site code automatically controls the filenames, but still not good.
       
        sql = (f"INSERT INTO images (image_root, site, sort_date, {file_exists_attribute}) "
                "VALUES(%s, %s, %s, %s) ON CONFLICT (image_root) DO UPDATE "
                f"SET {file_exists_attribute} = excluded.{file_exists_attribute};"
        )
        
        # These values will be fed into the sql command string (above)
        attribute_values = (
            base_filename, 
            site, 
            upload_time, 
            True
        )
    
   
    connection = None
    try:
        
        # Connect to database
        print('Establishing RDS connection...')
        connection = psycopg2.connect(database='ptrdatabase', user='ptrUser', password='ptrPassword', host='testdatabase.cb1rx8ymtxjb.us-east-1.rds.amazonaws.com', port = '5432')
        cursor = connection.cursor()
        print('Connection established.')

        print('Placing entry into ptr archive...')
        cursor.execute(sql,attribute_values)
        print('ENTRY INSERTED.')

        connection.commit()
        print('Done.')
    except (Exception, psycopg2.Error) as error :
        print(error)
    finally:
        #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed.')
    
    return True




def read_s3_body(bucket_name, object_name):
    """
    TODO: docstring
    """
    s3_c = boto3.client('s3',region_name='us-east-1')
    s3_object = s3_c.get_object(Bucket=bucket_name, Key=object_name)
    print('S3 connection established.')
    body = s3_object['Body']

    return body.read()
    
    
    
def parse_file(bucket,fname):
    """
    Parse a file from s3. Used to extract fits header values stored in a txt file.
    """
    
    print('Reading in file %s' % fname)
    data_entry = {}
    fits_line_length = 80
    
    contents = read_s3_body(bucket, fname)
    
    for i in range(0, len(contents), fits_line_length):
        single_header_line = contents[i:i+fits_line_length].decode('utf8')
        values = re.split('=|/|',single_header_line) # Split line twice according to '=' and '/' characters
        try:
            # Remove extra characters. Attribute values are first
            # stripped of single quotation marks before whitespace is removed.   
            attribute = values[0].strip()
            attribute_value = values[1].replace("'", "").strip() 
                
            data_entry[attribute] = attribute_value
        except:
            if attribute == 'END':
                break

    # Add the JSON representation of the data_entry to itself      
    print('Compiling data...')
    data_entry['JSON'] = json.dumps(data_entry)
    return data_entry
