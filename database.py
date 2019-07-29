# database.py

from utils import psql
import time, re
import boto3
from dotenv import load_dotenv
import os
from os.path import join, dirname


REGION = 'us-east-1'
BUCKET_NAME = 'photonranch-001'

dotenv_path = join(dirname(__file__), '.config')
load_dotenv(dotenv_path)

DB_USER = os.environ.get('DB_USER')
DB_USER_PASS = os.environ.get('DB_USER_PASS')

DB_HOST = os.environ.get('DB_HOST')
DB_IDENTIFIER = os.environ.get('DB_IDENTIFIER')
DB = os.environ.get('DB')


# Ensure RDS database instance is available for connection
print('\n' + '~~~~~~~~~~CONNECTING TO YOUR AWS-RDS DATABASE INSTANCES~~~~~~~~~~')
print('Recovering %s instance...' % DB_IDENTIFIER)

rds_c = boto3.client('rds', region_name=REGION)

running = True
while running:
    response = rds_c.describe_db_instances(DBInstanceIdentifier=DB_IDENTIFIER)
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

# Connect to database and execute SQL queries
if status == 'available':
    scan = psql.insert_all_header_files(DB, DB_USER, DB_USER_PASS, host, BUCKET_NAME)
else:
    print("Unable to connect to the database.")


