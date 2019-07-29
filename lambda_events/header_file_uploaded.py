import json
import urllib.parse
import boto3
import psycopg2
import re

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # context.callbackWaitsForEmptyEventLoop = False
    print('***RECIEVED S3 EVENT***')
    # Get the object from the event and show its content type
    BUCKET_NAME = event['Records'][0]['s3']['bucket']['name']
    FILE_NAME = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    sql = "INSERT INTO image_data(img_id, observer, site, capture_date, header) VALUES (%s, %s, %s, %s, %s)"
    connection = None
    try:
        print('Establishing RDS connection...')
        connection = psycopg2.connect(database='ptrdatabase', user='ptrUser', password='ptrPassword', host='testdatabase.cb1rx8ymtxjb.us-east-1.rds.amazonaws.com', port = '5432')
        cursor = connection.cursor()
        print('Connection established.')

        # INSERT DATA
        attribute_values = []
        
        data = parse_file(BUCKET_NAME, FILE_NAME)

        fname = data['FILENAME']
        site = re.split('-',fname)[0] # Extract site name from beginning of filename
        
        attribute_values.extend([
            data['FILENAME'],
            data['OBSERVER'],
            site,
            data['DATE-OBS'],
            data['JSON']
        ])
        
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
    s3_c = boto3.client('s3',region_name='us-east-1')
    s3_object = s3_c.get_object(Bucket=bucket_name, Key=object_name)
    print('S3 connection established.')
    body = s3_object['Body']

    return body.read()
    
def parse_file(bucket,fname):
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