
import pytest
import boto3
import os
import requests
import json

from lambda_service.info_images import info_table

from lambda_service.info_images import handle_info_image_created
from lambda_service.info_images import get_site_metadata
from lambda_service.info_images import get_info_channel

from lambda_service.tests.testutils import make_data_files
from lambda_service.tests.testutils import get_upload_url
from lambda_service.tests.testutils import upload_files
from lambda_service.tests.testutils import get_s3_event

TEST_BASE_FILENAME = "test-ptrdata-20200101-01234567"
TEST_SITE = "test"


@pytest.fixture(scope="module", autouse=True)
def setup_teardown_module():
    make_data_files()
    test_files_directory = 'lambda_service/tests/testing_data'
    files_to_upload = [
        (f"{TEST_BASE_FILENAME}-EX01.fits.bz2", f"{test_files_directory}/testdata.fits.bz2"),
        (f"{TEST_BASE_FILENAME}-EX10.fits.bz2", f"{test_files_directory}/testdata.fits.bz2"),
        (f"{TEST_BASE_FILENAME}-EX10.jpg", f"{test_files_directory}/testdata.jpg"),
        (f"{TEST_BASE_FILENAME}-EX11.jpg", f"{test_files_directory}/testdata.jpg"),
        (f"{TEST_BASE_FILENAME}-EX00.txt", f"{test_files_directory}/testdata.txt"),
    ]
    info_channel = 1
    upload_files(files_to_upload, 'info-images', info_channel)
    # make files
    yield
    # remove_data_files()


def add_site_metadata(site, channel, base_filename):
    info_table.put_item(Item={
        'pk': f'{site}#metadata',
        f'channel{channel}': base_filename
    })


def remove_site_metadata(pk):
    info_table.delete_item(Key={'pk': pk})


@pytest.fixture
def sitemetadata():
    """ Setup and teardown of the dynamodb info-images site#metadata entry """
    base_filename = TEST_BASE_FILENAME
    site = TEST_SITE
    channel = 1
    add_site_metadata(site, channel, base_filename)
    # this is where the test function runs
    yield
    # remove site metadata entry
    remove_site_metadata(f'{site}#metadata')



##### Unit Tests #####

def test_get_info_channel_exists():
    base_filename = 'test_base_filename'
    site_metadata = {
        'pk': 'ptr_data_unittest#metadata',
        'badchannel3': 'test_base_filename',
        'channel1': 'not the correct base_filename',
        'channel2': 'test_base_filename',
    }
    channel = get_info_channel(base_filename, site_metadata)
    assert (channel == 2)


def test_get_info_channel_missing():
    base_filename = 'test_base_filename'
    site_metadata = {
        'pk': 'ptr_data_unittest#metadata',
        'badchannel3': 'test_base_filename',
        'channel1': 'not the correct base_filename',
    }
    channel = get_info_channel(base_filename, site_metadata)
    assert (channel is None)



##### Integration Tests #####

def test_new_info_jpg():

    # make sure the site metadata exists for our test object so the image is saved in the right channel
    channel = 1
    site = TEST_SITE
    base_filename = TEST_BASE_FILENAME
    add_site_metadata(site, channel, base_filename)

    small_jpg_filepath = f"info-images/{base_filename}-EX11.jpg"
    event = get_s3_event(small_jpg_filepath)

    # test the function
    handle_info_image_created(event, {})

    # check that the info-images dynamodb entry was created correctly
    response = info_table.get_item(Key={"pk": f"{site}#{channel}"})
    print(response)
    assert response['Item']


def test_new_info_txt():

    # make sure the site metadata exists for our test object so the image is saved in the right channel
    channel = 1
    site = TEST_SITE
    base_filename = TEST_BASE_FILENAME
    add_site_metadata(site, channel, base_filename)

    txt_filepath = f"info-images/{base_filename}-EX00.txt"
    event = get_s3_event(txt_filepath)

    # test the function
    handle_info_image_created(event, {})

    # Check that the header data was saved for our object
    response = info_table.get_item(Key={"pk": f"{site}#{channel}"})
    print(response)
    assert 'header' in response['Item']


def test_get_site_metadata(sitemetadata):
    base_filename = TEST_BASE_FILENAME
    site = TEST_SITE
    site_metadata = get_site_metadata(site)
    assert(site_metadata['channel1'] == base_filename)
