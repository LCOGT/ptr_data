# psql.py

from utils import aws
import boto3
import psycopg2
import re


def insert_all_header_files(db, db_user, db_user_pass, db_host, bucket):
    print('\nINSERTING DATA FROM S3...')
    full_scan_complete = False
    scanning_error = False

    connection = None
    try:
        connection = psycopg2.connect(database=db, user=db_user, password=db_user_pass, host=db_host, port="5432")
        cursor = connection.cursor()

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print('You are connected to - ', record, '\n')
        
        # Clear old entries in database before rescanning
        cursor.execute("DELETE FROM image_data") 
        print('Database cleared.')

        # Insert all header file data
        data = aws.scan_s3_image_data(bucket, file_suffix='E00.txt')
        for header in data:
            scan = insert_header_file(header, cursor)
            if scan == False:
                scanning_error = True
        connection.commit()
        
        # Report on scanning results
        if scanning_error == False:
            print("Scanning completed successfully!")
            full_scan_complete = True
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

    return full_scan_complete


def insert_header_file(header_data, cursor):
    scan_complete = False
    sql = "INSERT INTO image_data(img_id, observer, site, capture_date, header) VALUES (%s, %s, %s, %s, %s)"

    fname = header_data['FILENAME']
    site = re.split('-',fname)[0] # Extract site name from beginning of filename
    
    # Format row for SQL insertion
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


def get_last_modified(site, k):
    connection = None
    try:
        connection = psycopg2.connect(database=db, user=db_user, password=db_user_pass, host=db_host, port="5432")
        cursor = connection.cursor()

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print('You are connected to - ', record, '\n')
        
    except (Exception, psycopg2.Error) as error :
        print ("\nError while connecting to PostgreSQL:", error)
    finally:
        # Closing database connection
        if(connection):
            cursor.close()
            connection.close()
            print("\nPostgreSQL connection is closed.")
    except (Exception, psycopg2.Error) as error :
        print ("\nError while connecting to PostgreSQL:", error)
    finally:
        # Closing database connection
        if(connection):
            cursor.close()
            connection.close()
            print("\nPostgreSQL connection is closed.")