# psql.py

import boto3
import psycopg2
import re
from utils import aws

REGION = 'us-east-1'
BUCKET_NAME = 'photonranch-001'
URL_EXPIRATION = 3600 # Seconds until URL expiration

def insert_all_header_files(db, db_user, db_user_pass, db_host):
    scan_complete = False
    scanning_error = False

    connection = None
    try:
        # Establish connection to the ptr archive
        connection = psycopg2.connect(database=db, user=db_user, password=db_user_pass, host=db_host, port="5432")
        cursor = connection.cursor()

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("\nYou are connected to - ", record, "\n")
        
        # Clear old entries in database before rescanning
        cursor.execute("DELETE FROM image_data") 
        print('***WARNING: DATABASE CLEARED***')

        # Insert header file data into the ptr archive
        data = aws.scan_s3_image_data(BUCKET_NAME,file_suffix='E00.txt')
        for header in data:
            scan = insert_header_file(header, cursor)
            if scan == False:
                scanning_error = True
        connection.commit()
        
        # Report on scanning results
        if scanning_error == False:
            print("Scanning completed successfully!")
            scan_complete = True
        else:
            print("Scanning completed with errors.")

    except (Exception, psycopg2.Error) as error :
        print ("\nError while connecting to PostgreSQL:", error)
    finally:
        # Closing database connection
        if(connection):
            cursor.close()
            connection.close()
            print("\nPostgreSQL connection is closed.")

    return scan_complete

def insert_header_file(header_data, cursor):
    scan_complete = False
    sql = "INSERT INTO image_data(img_name, observer, site, last_modified, header) VALUES (%s, %s, %s, %s, %s)"

    fname = header_data['FILENAME']
    site = re.split('-',fname)[0] # Extract site name from beginning of filename

    attribute_values = []
    attribute_values.extend([
        header_data['FILENAME'],
        header_data['OBSERVER'],
        site,
        header_data['DATE-OBS'],
        header_data['JSON']
    ])

    try:
        cursor.execute(sql,attribute_values)
        print('---> ENTRY INSERTED: ' + fname)
        scan_complete = True
    except (Exception, psycopg2.Error) as error :
        print("Error while inserting row to ptr archive:", error)

    return scan_complete