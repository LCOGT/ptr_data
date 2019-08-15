# database.py

import db_init
from utils import psql, aws

import boto3
import psycopg2
import time


params = db_init.config()
db_params = params['postgresql']
aws_params = params['aws']

BUCKET = aws_params['bucket']
REGION = aws_params['region']
DB_IDENTIFIER = aws_params['db_identifier']


def connect_to_rds():
    print('\n{:*^80}'.format('PTR Archive'))
    connection = None


    print('Connecting to database...')
    connection = psycopg2.connect(**db_params)

    cursor = connection.cursor()

    print('Database Version:')
    cursor.execute('SELECT version()')

    db_version = cursor.fetchone()
    print(db_version)

    # check status of database
    status = check_db_status(DB_IDENTIFIER)
    print('{:*^80}'.format(''))

    if status == 'available':

        # EXECUTE SQL QUERIES
        #####################################################

        #psql.delete_all_entries(cursor, connection)
        psql.insert_all_entries(cursor, connection, BUCKET)

        #####################################################
    else: 
        print('Unable to connect to database.')
    
    cursor.close()

    if connection is not None:
        connection.close()
        print('Connection closed')

def check_db_status(db_identifier):
    status = None
    rds_c = boto3.client('rds')

    # identify database instance from rds and check status
    running = True
    while running:
        response = rds_c.describe_db_instances(DBInstanceIdentifier=db_identifier)
        db_instances = response['DBInstances']

        if len(db_instances) != 1:
            raise Exception('More than one DB instance returned.')

        db_instance = db_instances[0]

        status = db_instance['DBInstanceStatus']
        print('DB status: %s' % status)

        time.sleep(3)
        if status == 'available':
            endpoint = db_instance['Endpoint']
            host = endpoint['Address']
            print('DB instance ready with host: %s' % host)

            running = False
    
    return status

if __name__ == "__main__":
    connect_to_rds()

