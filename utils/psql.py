# psql.py

from utils import aws
import boto3
import psycopg2
import re


def insert_all_header_files(cursor, connection):
    bucket = 'photonranch-001' # TODO: have s3 variables be read in in aws.py instead of passing through bucket and region names
    full_scan_complete = False
    scanning_error = False

    try:
        # clear old entries in database before rescanning
        cursor.execute("DELETE FROM images") 
        print('\n{:^80}\n'.format('**DATABASE CLEARED**'))

        # scan and insert header file data
        data = aws.scan_s3_image_data(bucket, file_suffix='E00.txt')

        print('\nInserting data...')
        for header in data:
            scan = insert_header_file(header, cursor)
            if scan == False:
                scanning_error = True
        connection.commit()
        
        # report on scanning results
        if scanning_error == False:
            print("\nScanning completed successfully!")
            full_scan_complete = True
        else:
            print("\nScanning completed with errors.")

    except (Exception, psycopg2.Error) as error :
        print ("\nError while connecting to PostgreSQL:", error)

    return full_scan_complete


def insert_header_file(header_data, cursor):
    """
    Insert a single record into the ptr image database

    NOTE: This is the only function that needs modification
          when changing the database schema or attribute types! 
          (Make sure to the update_ptr_archive lambda function to match)
    """
    scan_complete = False

    sql = "INSERT INTO images(image_root, observer, site, capture_date, sort_date, right_ascension, header) VALUES (%s, %s, %s, %s, %s, %s, %s)"

    # extract values from header data
    image_root = header_data['FILENAME']
    observer = header_data['OBSERVER']
    site = header_data['FILENAME']
    capture_date = header_data['DATE-OBS']
    header = header_data['JSON']
    right_ascension = header_data['MNT-RA']

    # extra attribute formatting
    image_root = re.sub('.fits', '', image_root) # remove file extension
    image_root = image_root[:-4] # remove image tag

    site = re.split('-',site)[0] # extract site name from beginning of filename

    capture_date = re.sub('T', ' ', capture_date) # format capture time as SQL timestamp

    # format row for SQL insertion
    attribute_values = [
        image_root,
        observer,
        site,
        capture_date,
        capture_date, # this one applies to the sort_date attrbute
        right_ascension,
        header
    ]

    try:
        cursor.execute(sql,attribute_values)
        print('-> ENTRY INSERTED: ' + image_root)
        scan_complete = True
    except (Exception, psycopg2.Error) as error :
        print("Error while inserting row to ptr archive:", error)

    return scan_complete


def get_last_modified(cursor, connection, k):
    sql = "SELECT image_root FROM images ORDER BY capture_date DESC LIMIT %d" % k
    try:
        cursor.execute(sql)
        images = cursor.fetchmany(k)
    except (Exception, psycopg2.Error) as error :
        print("Error while retrieving records:", error)

    return images

def query_database(cursor, query):
    sql = "SELECT image_root FROM images"

    if len(query) > 0:
        sql = sql + " WHERE"
        for k, v in query.items():
            add = " %s = '%s' AND" % (k ,v)
            sql = sql + add

        sql = sql[:-3]

    print(sql)
    try:
        cursor.execute(sql)
        images = cursor.fetchall()
    except (Exception, psycopg2.Error) as error :
        print("Error while retrieving records:", error)

    return images