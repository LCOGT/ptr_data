# database.py

from utils import helpers
import psycopg2
import time, requests
import boto3

API = "http://localhost:5000"
REGION = 'us-east-1'
BUCKET_NAME = 'photonranch-001'

db_identifier = 'testdatabase'
rds_c = boto3.client('rds', region_name=REGION)
s3_r = boto3.resource('s3', region_name=REGION)
s3_c = boto3.client('s3', region_name=REGION)

# Create new database instance
# response = rds.create_db_instance(
#     DBInstanceIdentifier = 'testDatabase',
#     MasterUsername = 'ptrUser',
#     MasterUserPassword = 'ptrPassword',
#     DBInstanceClass = 'db.t2.micro',
#     Engine = 'postgres',
#     AllocatedStorage = 5)
# print(response)

# List all database instances associated with AWS account on rds
print('\n' + '~~~~~~~~~~CONNECTING TO YOUR AWS-RDS DATABASE INSTANCES~~~~~~~~~~')
try:
    dbs = rds_c.describe_db_instances()
    print('Database Instance(s) Found:')
    for db in dbs['DBInstances']:
        print(("%s@%s:%s %s") %  (db['MasterUsername'], db['Endpoint']['Address'],db['Endpoint']['Port'], db['DBInstanceStatus']))
except Exception as e:
    print(e)

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
    ############################################################################################################### NEW CODE
    # Print tables in the databases
    cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
    print('LIST OF TABLES IN DATABASE:')
    for table in cursor.fetchall():
        print(table)

    # Scan S3 for meta-data text files
    entries = []
    bucket = s3_r.Bucket(BUCKET_NAME)
    print('\nSEARCHING FOR IMAGE META-DATA STORED IN BUCKET: %s' % BUCKET_NAME)
    for key in helpers.get_matching_s3_objects(BUCKET_NAME,suffix='E00.txt'):
        filename = key['Key']
        
        #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
 
        #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
        #print(key['Key'])
        
        #data = s3_c.get_object(Bucket='my_s3_bucket', Key=filename)
        #this_file = bucket.Object(key)
        #print(this_file)
        #data = this_file['Body']
        #print(data)
        # this_file.get()['Body'].read().decode('utf-8')
        #print(data)
        #print('I GOT HERE?')
        #entries.append(data.split())
 
    ############################################################################################################### NEW CODE

except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL:", error)
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

# Function to create tables in database
# def create_tables():
#     # provide sql statements
#     commands = (
#         """
#         CREATE TABLE images (
#             img_id SERIAL PRIMARY KEY,
#             img_name VARCHAR(255) NOT NULL
#             )
#         """
#         )

