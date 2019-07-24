# database.py

from utils import helpers
import psycopg2
import time, re
import boto3
import urllib.request
from dotenv import load_dotenv
from os.path import join, dirname


#BUCKET_NAME = 'photonranch-001'
#DB_IDENTIFIER = 'testdatabase'

dotenv_path = join(dirname(__file__), '.config')
load_dotenv(dotenv_path)
REGION = os.environ.get('REGION')
BUCKET_NAME = os.environ.get('BUCKET_NAME')

USER = os.environ.get('USER')
USER_PASS = os.environ.get('USER_PASS')

HOST = os.environ.get('HOST')
DB_IDENTIFIER = os.environ.get('DB_IDENTIFIER')
DB = os.environ.get('DB')
TABLE = os.environ.get('TABLE')

FILE_PREFIX = os.environ.get('FILE_PREFIX')
FILE_SUFFIX = os.environ.get('FILE_SUFFIX')

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

helpers.db_connect(DB_IDENTIFIER)
data = helpers.scan_s3_image_data(BUCKET_NAME,file_suffix='E00.txt')
                
for k, v in data[0].items():
    print(k, ":", v)
print('***')

