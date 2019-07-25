# aws.py

import boto3
import psycopg2
from botocore.client import Config
import time, re, json
from utils import aws

REGION = 'us-east-1'
BUCKET_NAME = 'photonranch-001'
URL_EXPIRATION = 3600 # Seconds until URL expiration

rds_c = boto3.client('rds', region_name=REGION)

def db_connect(db_identifier, db, db_user, db_user_pass, db_host):
    # Recover sepcific database through an identifier
    print('\n' + '************************************************************************')
    print('Recovering %s instance...' % db_identifier)
    running = True
    while running:
        response = rds_c.describe_db_instances(DBInstanceIdentifier=db_identifier)
        db_instances = response['DBInstances']

        if len(db_instances) != 1:
            raise Exception('More than one DB instance returned; make sure all database instances have unique identifiers')

        db_instance = db_instances[0]

        status = db_instance['DBInstanceStatus']
        print('DB status: %s' % status)

        time.sleep(5)
        if status == 'available':
            endpoint = db_instance['Endpoint']
            host = endpoint['Address']

            print('DB instance ready with host: %s' % host)
            running = False
    ### INSERTING DATA
    
    sql = "INSERT INTO image_data(img_name, observer, site, header) VALUES (%s, %s, %s, %s)"
    connection = None
    try:
        connection = psycopg2.connect(database=db, user=db_user, password = db_user_pass, host = db_host, port = "5432")
        cursor = connection.cursor()
        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record)
        print('************************************************************************' + '\n')

        # INSERT DATA
        cursor.execute("DELETE FROM image_data") # Clear old entries before scanning and adding new entries in
        print('***WARNING: DATABASE CLEARED***')
        print('\n INSERTING DATA INTO DATABASE:')

        data = aws.scan_s3_image_data(BUCKET_NAME,file_suffix='E00.txt')
        for d in data:
            attribute_values = []
            attribute_values.extend([
                d['FILENAME'],
                d['OBSERVER'],
                'WMD',
                d['JSON']
            ])
            fname = d['FILENAME']

            cursor.execute(sql,attribute_values)
            print('---> ENTRY INSERTED: ' + fname)

        connection.commit()
    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL:", error)
    finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")