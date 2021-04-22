
import pytest

from lambda_service.db import header_data_exists
from lambda_service.db import update_new_image
from lambda_service.db import update_header_data
from lambda_service.db import db_remove_base_filename
from lambda_service.db import get_session, Image
from lambda_service.db import DB_ADDRESS

TEST_BASE_FILENAME = 'tst-test-20200924-00000041'

def delete_test_entries(base_filename):
    with get_session(db_address=DB_ADDRESS) as session:
        session.query(Image).filter(Image.base_filename==base_filename).delete()

@pytest.fixture 
def setup_teardown():
  delete_test_entries(TEST_BASE_FILENAME)
  yield
  delete_test_entries(TEST_BASE_FILENAME)


def test_header_data_exists_no_header(setup_teardown):

    # Create new db entry
    with get_session(db_address=DB_ADDRESS) as session:
        new_image = Image(
            base_filename=TEST_BASE_FILENAME,
            site="tst",
            data_type="EX"
        )
        session.add(new_image)
        session.commit()

    # Should not have a header entry yet
    assert not header_data_exists(DB_ADDRESS, TEST_BASE_FILENAME)


def test_header_data_exists_includes_header(setup_teardown):

    # Create new db entry
    with get_session(db_address=DB_ADDRESS) as session:
        new_image = Image(
            base_filename=TEST_BASE_FILENAME,
            site="tst",
            data_type="EX"
        )
        session.add(new_image)
        session.commit()

    # Example header we'll use
    header = {
        "JSON": 'some data',
        "OBJCTRA": 12.34,
        "OBJCTDEC": 56.78,
        "ALTITUDE": 12.34,
        "AZIMUTH": 56.78,
        "FILTER": 'test_filter',
        "AIRMASS": 1.234,
        "EXPTIME": 1,
        "USERID": 'test_userid',
        "USERNAME": 'test_user',
        "DATE-OBS": '2021-01-01T12:34.54'
    }

    # Add simple header contents
    update_header_data(DB_ADDRESS, TEST_BASE_FILENAME, 'EX', header)

    assert header_data_exists(DB_ADDRESS, TEST_BASE_FILENAME)


def test_db_remove_base_filename(setup_teardown):

    # Create new db entry
    with get_session(db_address=DB_ADDRESS) as session:
        new_image = Image(
            base_filename=TEST_BASE_FILENAME,
            site="tst",
            data_type="EX"
        )
        session.add(new_image)
        session.commit()

    # Verify entry exists
    with get_session(db_address=DB_ADDRESS) as session:
        assert session.query(Image.image_id)\
            .filter_by(base_filename=TEST_BASE_FILENAME)\
            .scalar() is not None

    # Remove the entry
    db_remove_base_filename(TEST_BASE_FILENAME)
    
    # Verify entry no longer exists
    with get_session(db_address=DB_ADDRESS) as session:
        assert session.query(Image.image_id)\
            .filter_by(base_filename=TEST_BASE_FILENAME)\
            .scalar() is None


#def test_update_header_data():
    #pass


#def test_update_new_image():
    #pass
