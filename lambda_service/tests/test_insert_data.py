import pytest
import boto3
import os
import time

from lambda_service.db import get_session
from lambda_service.db import Image
from lambda_service.db import DB_ADDRESS
from lambda_service.insert_data import handle_s3_object_created

from lambda_service.tests.testutils import make_data_files
from lambda_service.tests.testutils import get_upload_url
from lambda_service.tests.testutils import upload_files
from lambda_service.tests.testutils import get_s3_event

# These are replicas of the notification event passed into our lambda function
# whenever an object is created in s3. 
# The files they refer to should already exist in <s3_bucketname>/test/

TEST_BASE_FILENAME = 'tst-test-20200924-00000041'
TEST_SITE = 'test'

text_file_key = f"{TEST_SITE}/{TEST_BASE_FILENAME}-e00.txt"
raw_fits_file_key = f"{TEST_SITE}/{TEST_BASE_FILENAME}-e01.fits.bz2"
reduced_fits_file_key = f"{TEST_SITE}/{TEST_BASE_FILENAME}-e10.fits.bz2"
reduced_jpg_file_key = f"{TEST_SITE}/{TEST_BASE_FILENAME}-e10.jpg"

s3_event_fits_header = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-1', 'eventTime': '2020-09-24T17:40:17.597Z', 'eventName': 'ObjectCreated:Post', 'userIdentity': {'principalId': 'AWS:AROAUOVR2PJKRTRQ7MH2Y:photonranch-api-dev-upload'}, 'requestParameters': {'sourceIPAddress': '70.185.182.123'}, 'responseElements': {'x-amz-request-id': '0B8BE5681BA6113A', 'x-amz-id-2': 'BZZqJALmCdeYTDtNgb8uBMF+IydC2LAmNM06WDU2WVQUtC+FBU1tzS/uUIWp3EHVa2qhfnNy2kPBanhWJmrhHYPoxclwTOEy'}, 's3': {'s3SchemaVersion': '1.0', 'configurationId': 'exS3-v2--4f5bbfbc57aa111afcf3fd723f36e652', 'bucket': {'name': 'photonranch-001', 'ownerIdentity': {'principalId': 'A1OU8433EUT16S'}, 'arn': 'arn:aws:s3:::photonranch-001'}, 'object': {'key': text_file_key, 'size': 960, 'eTag': '3738966ce6259483abd06998d5433089', 'sequencer': '005F6CDA03E5828FAC'}}}]}
s3_event_ex10_jpg = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-1', 'eventTime': '2020-09-24T17:40:18.733Z', 'eventName': 'ObjectCreated:Post', 'userIdentity': {'principalId': 'AWS:AROAUOVR2PJKRTRQ7MH2Y:photonranch-api-dev-upload'}, 'requestParameters': {'sourceIPAddress': '70.185.182.123'}, 'responseElements': {'x-amz-request-id': 'F96A5D84B8F7B9D6', 'x-amz-id-2': 'yki1eZc0/t4UvlFAp1llOTsbXHedFvJRbki2W3NSOUBEPJ42uVGkgC7F3NSbklPS60uU7DVMsxtWDUEftINkapisCZ34g23R'}, 's3': {'s3SchemaVersion': '1.0', 'configurationId': 'exS3-v2--4f5bbfbc57aa111afcf3fd723f36e652', 'bucket': {'name': 'photonranch-001', 'ownerIdentity': {'principalId': 'A1OU8433EUT16S'}, 'arn': 'arn:aws:s3:::photonranch-001'}, 'object': {'key': reduced_jpg_file_key, 'size': 98987, 'eTag': '268182655972e76fefb712c5585993e8', 'sequencer': '005F6CDA04CBC98813'}}}]}
s3_event_ex10_fits = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-1', 'eventTime': '2020-09-24T17:40:20.446Z', 'eventName': 'ObjectCreated:Post', 'userIdentity': {'principalId': 'AWS:AROAUOVR2PJKRTRQ7MH2Y:photonranch-api-dev-upload'}, 'requestParameters': {'sourceIPAddress': '70.185.182.123'}, 'responseElements': {'x-amz-request-id': '3FA34BB173CB6E6A', 'x-amz-id-2': 'UYM3Td4NBXvCz0NIwRWHi87hnqUriu8wbJ6fCnc9Jaz7NpgVw9SAcl/Lp/6GVD55gwz87HglXHURRk0mkFF1HW2La8ysAAzzeaOcXWTCUQE='}, 's3': {'s3SchemaVersion': '1.0', 'configurationId': 'exS3-v2--4f5bbfbc57aa111afcf3fd723f36e652', 'bucket': {'name': 'photonranch-001', 'ownerIdentity': {'principalId': 'A1OU8433EUT16S'}, 'arn': 'arn:aws:s3:::photonranch-001'}, 'object': {'key': reduced_fits_file_key, 'size': 272847, 'eTag': 'c3cd3800516bc3675c1fd5d804c32556', 'sequencer': '005F6CDA063AAA22E3'}}}]}
s3_event_ex01_fits = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-1', 'eventTime': '2020-09-24T17:40:36.389Z', 'eventName': 'ObjectCreated:Post', 'userIdentity': {'principalId': 'AWS:AROAUOVR2PJKRTRQ7MH2Y:photonranch-api-dev-upload'}, 'requestParameters': {'sourceIPAddress': '70.185.182.123'}, 'responseElements': {'x-amz-request-id': 'A83B04B0F73247AA', 'x-amz-id-2': '6+QXWTf1BDoJmOUdGtMfAGJLrtDvHQrjAOtAP/JctAjQoudCTvzIdAClkxN0SaF4RslRrv1ewsyrezVXZTTwOhRYW5ZtbxtU'}, 's3': {'s3SchemaVersion': '1.0', 'configurationId': 'exS3-v2--4f5bbfbc57aa111afcf3fd723f36e652', 'bucket': {'name': 'photonranch-001', 'ownerIdentity': {'principalId': 'A1OU8433EUT16S'}, 'arn': 'arn:aws:s3:::photonranch-001'}, 'object': {'key': raw_fits_file_key, 'size': 32797977, 'eTag': 'f68547c12a881cfd9ac6573b9341b4be', 'sequencer': '005F6CDA07DD59DA61'}}}]}

@pytest.fixture(scope="module", autouse=True)
def setup_teardown_module():
    start = time.time()
    data = None
    header = {"headkey": "headval"}
    test_files_directory = 'lambda_service/tests/testing_data'
    make_data_files(data, header, test_files_directory)
    print(time.time() - start)
    files_to_upload = [
        #(f"{TEST_BASE_FILENAME}-EX01.fits.bz2", f"{test_files_directory}/testdata.fits.bz2"),
        #(f"{TEST_BASE_FILENAME}-EX10.fits.bz2", f"{test_files_directory}/testdata.fits.bz2"),
        #(f"{TEST_BASE_FILENAME}-EX10.jpg", f"{test_files_directory}/testdata.jpg"),
        ##(f"{TEST_BASE_FILENAME}-EX11.jpg", #f"{test_files_directory}/testdata.jpg"),
        #(f"{TEST_BASE_FILENAME}-EX00.txt", f"{test_files_directory}/testdata.txt"),

        (raw_fits_file_key, f"{test_files_directory}/testdata.fits.bz2"),
        (reduced_fits_file_key, f"{test_files_directory}/testdata.fits.bz2"),
        (reduced_jpg_file_key, f"{test_files_directory}/testdata.jpg"),
        (text_file_key, f"{test_files_directory}/testdata.txt"),
    ]
    upload_files(files_to_upload, 'info-images')
    print(time.time() - start)

    # make sure that we manually add and check the database (instead of the production lambda trigger)
    delete_test_entries(TEST_BASE_FILENAME)
    print(time.time() - start)
    yield
    print(time.time() - start)
    # remove_data_files()
    #delete_test_entries(TEST_BASE_FILENAME)
    print(time.time() - start)


def delete_test_entries(base_filename):
    with get_session(db_address=DB_ADDRESS) as session:
        s = session.query(Image).filter(Image.base_filename==base_filename).delete()

@pytest.fixture(scope="function", autouse=True) 
def clear_db_entries_for_each_test():
    delete_test_entries(TEST_BASE_FILENAME)
    yield
    delete_test_entries(TEST_BASE_FILENAME)
    
def add_01fits():
    #s3_event_ex01_fits = get_s3_event()
    handle_s3_object_created(s3_event_ex01_fits, {})

def add_10jpg():
    handle_s3_object_created(s3_event_ex10_jpg, {})

def add_header():
    handle_s3_object_created(s3_event_fits_header, {})

def add_10fits():
    handle_s3_object_created(s3_event_ex10_fits, {})


##############################
##########  Tests  ###########
##############################

def test_handle_s3_object_created_only_header():
    add_header()
    with get_session(db_address=DB_ADDRESS) as session:
        entry = session.query(Image)\
            .filter(Image.base_filename==TEST_BASE_FILENAME)\
            .one()
        assert entry.header and not entry.jpg_medium_exists

def test_handle_s3_object_created_only_jpg():
    add_10jpg()
    with get_session(db_address=DB_ADDRESS) as session:
        entry = session.query(Image)\
            .filter(Image.base_filename==TEST_BASE_FILENAME)\
            .one()
        assert entry.jpg_medium_exists and not entry.header


def test_handle_s3_object_created_only_fits():
    add_10fits()
    with get_session(db_address=DB_ADDRESS) as session:
        entry = session.query(Image)\
            .filter(Image.base_filename==TEST_BASE_FILENAME)\
            .one()
        assert entry.fits_10_exists 
        assert entry.header is not None


def test_handle_s3_object_created_all_files():
    add_header()
    add_10jpg()
    add_01fits()
    add_10fits()
    with get_session(db_address=DB_ADDRESS) as session:
        entry = session.query(Image)\
            .filter(Image.base_filename==TEST_BASE_FILENAME)\
            .one()
        assert entry.fits_10_exists and entry.header
