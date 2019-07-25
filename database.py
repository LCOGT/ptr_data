# database.py

from utils import helpers
import psycopg2
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

rds_c = boto3.client('rds', region_name=REGION)

# List all database instances associated with AWS account on rds
print('\n' + '~~~~~~~~~~CONNECTING TO YOUR AWS-RDS DATABASE INSTANCES~~~~~~~~~~')
try:
    dbs = rds_c.describe_db_instances()
    print('Database Instance(s) Found:')
    for db in dbs['DBInstances']:
        print(("%s@%s:%s %s") %  (db['MasterUsername'], db['Endpoint']['Address'],db['Endpoint']['Port'], db['DBInstanceStatus']))
except Exception as e:
    print(e)

data = helpers.scan_s3_image_data(BUCKET_NAME,file_suffix='E00.txt')
helpers.db_connect(DB_IDENTIFIER, DB, DB_USER, DB_USER_PASS, DB_HOST, data)

                
# for k, v in data[0].items():
#     print(k, ":", v)
# print('***')

