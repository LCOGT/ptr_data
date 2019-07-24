# database.py

from utils import helpers
import psycopg2
import time, re
import boto3
import urllib.request


BUCKET_NAME = 'photonranch-001'
DB_IDENTIFIER = 'testdatabase'

#if __name__ == "main":

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

