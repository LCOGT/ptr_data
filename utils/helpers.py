# helpers.py

import boto3
import psycopg2
from botocore.client import Config
import time, re


#REGION = 'us-east-1'
URL_EXPIRATION = 3600 # Seconds until URL expiration

dotenv_path = join(dirname(__file__), '../.config')
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
s3_c = boto3.client('s3', region_name=REGION)
s3_r = boto3.resource('s3', region_name=REGION)

# Code from https://alexwlchan.net/2018/01/listing-s3-keys-redux/
def get_matching_s3_objects(bucket, prefix='', suffix=''):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)

        try:
            contents = resp['Contents']
        except KeyError:
            return

        for obj in contents:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield obj

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

def read_s3_body(bucket_name, object_name):
    s3_object = s3_c.get_object(Bucket=bucket_name, Key=object_name)
    body = s3_object['Body']

    return body.read()

def scan_s3_image_data(bucket, file_prefix='', file_suffix=''):
    # Scan S3 for meta-data text files
    data = []
    print('\nSEARCHING FOR IMAGE META-DATA STORED IN BUCKET: %s' % bucket)
    for key in get_matching_s3_objects(bucket, prefix=file_prefix, suffix=file_suffix): # Iterate through every matching text file in S3 bucket
        path = key['Key']
        contents = read_s3_body(bucket, path)
        fits_line_length = 80

        file_data = {}
        print('SCANNING: ' + path)
        for i in range(0, len(contents), fits_line_length): # Iterate through lines in file
            single_header_line = contents[i:i+fits_line_length].decode('utf8')

            # Split line twice according to '=' and '/' characters
            values = re.split('=|/|',single_header_line)
            
            try:
                # Remove extra characters   
                attribute = values[0].strip()
                attribute_value = values[1].replace("'", "").strip() # Attribute values are first stripped of single quotation marks before whitespace is removed.
                
                file_data[attribute] = attribute_value
            except:
                if attribute == 'END':
                    break

        data.append(file_data)

    return data

def db_connect(db_identifier):
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
            print('************************************************************************' + '\n')
            running = False

    try:
        connection = psycopg2.connect(database="ptrdatabase", user = "ptrUser", password = "ptrPassword", host = "testdatabase.cb1rx8ymtxjb.us-east-1.rds.amazonaws.com", port = "5432")
        cursor = connection.cursor()
        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record,"\n")

        # Print tables in the databases
        cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
        print('LIST OF TABLES IN DATABASE:')
        for table in cursor.fetchall():
            print(table)

    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL:", error)
    finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")
