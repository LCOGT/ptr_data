# database.py

import db_init
from utils import psql

import boto3
import psycopg2
import time



REGION = 'us-east-1'
BUCKET_NAME = 'photonranch-001'


def connect_to_rds():
    print('\n{:*^80}'.format('PTR Archive'))
    connection = None
    try:
        params = db_init.config()
        db_params = params['postgresql']
        aws_params = params['aws']

        print('Connecting to database...')
        connection = psycopg2.connect(**db_params)

        cursor = connection.cursor()

        print('Database Version:')
        cursor.execute('SELECT version()')

        db_version = cursor.fetchone()
        print(db_version)

        # check status of database
        db_identifier = aws_params['db_identifier']
        status = check_db_status(db_identifier)
        print('{:*^80}'.format(''))

        if status == 'available':
            # EXECUTE SQL QUERIES
            #####################################################

            #psql.insert_all_header_files(cursor, connection)
            query = {
                "site": "TST",
            }
            print(psql.query_database(cursor, query))

            #####################################################
        else: 
            print('Unable to connect to database.')
        
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
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

