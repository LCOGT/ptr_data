import logging
import os
import json
import boto3
import re
from datetime import datetime
from http import HTTPStatus
from contextlib import contextmanager
from sqlalchemy import Column, String, Integer, Float, Boolean, ForeignKey, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.engine.url import URL # don't need if we get the db-address from aws ssm.
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.exc import ArgumentError

from lambda_service.helpers import get_s3_image_path, get_s3_file_url, get_site_from_base_filename

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

Base = declarative_base()

ssm = boto3.client('ssm')
def get_secret(key):
    """
    Some parameters are stored in AWS Systems Manager Parameter Store.
    This replaces the .env variables we used to use with flask.
    """
    resp = ssm.get_parameter(
    	Name=key,
    	WithDecryption=True
    )
    return resp['Parameter']['Value']

DB_ADDRESS = get_secret('db-url')


@contextmanager
def get_session(db_address):
    """ Get a connection to the database.

    Returns:
        session: SQLAlchemy Database Session
    """
    engine = create_engine(db_address)
    db_session = sessionmaker(bind=engine)
    session = db_session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

def keyvalgen(obj):
    """ Generate attr name/val pairs, filtering out SQLA attrs."""
    excl = ('_sa_adapter', '_sa_instance_state')
    for k, v in vars(obj).items():
        if not k.startswith('_') and not any(hasattr(v, a) for a in excl):
            yield k, v

class Image(Base):
    __tablename__ = 'images'

    image_id          = Column(Integer, primary_key=True)
    base_filename     = Column(String)
    data_type         = Column(String)
    site              = Column(String)
    capture_date      = Column(DateTime, default=datetime.utcnow)
    sort_date         = Column(DateTime, default=datetime.utcnow)
    right_ascension   = Column(Float)
    declination       = Column(Float)
    altitude          = Column(Float)
    azimuth           = Column(Float)
    filter_used       = Column(String)
    airmass           = Column(Float)
    exposure_time     = Column(Float)
    username          = Column(String)
    user_id           = Column(String)
    header            = Column(String)

    fits_01_exists    = Column(Boolean)
    fits_10_exists    = Column(Boolean)
    jpg_medium_exists = Column(Boolean)



    def __init__(self, **kwargs):
        super(Image, self).__init__(**kwargs)

    def __repr__(self):
        params = ', '.join(f'{k}={v}' for k, v in keyvalgen(self))
        return f"{self.__class__.__name__}({params})"

    def get_image_pkg(self):
        """ A dictionary representation of common image metadata.

        This is the format that is used and expected by the frontend when it 
        queries this api for images. 

        Notably missing from this is the entire fits header, for smaller 
        payload sizes. 
        
        """

        package = {
            "image_id": self.image_id,
            "base_filename": self.base_filename,
            "data_type": self.data_type,
            "site": self.site, 

            "exposure_time": self.exposure_time,
            "filter_used": self.filter_used,
            "right_ascension": self.right_ascension, 
            "declination": self.declination, 
            "azimuth": self.azimuth,
            "altitude": self.altitude,
            "airmass": self.airmass,

            "fits_01_exists": self.fits_01_exists,
            "fits_10_exists": self.fits_10_exists,
            "jpg_medium_exists": self.jpg_medium_exists,

            "username": self.username,
            "user_id": self.user_id,
        }

        # Convert to timestamp in milliseconds
        package["capture_date"] = int(1000 * self.capture_date.timestamp())
        package["sort_date"] = int(1000 * self.sort_date.timestamp())

        # Include a url to the jpg
        package["jpg_url"] = ""
        if self.jpg_medium_exists:
            path = get_s3_image_path(self.base_filename, self.data_type, "10", "jpg")
            url = get_s3_file_url(path)
            package["jpg_url"] = url

        return package


def header_data_exists(db_address, base_filename):
    with get_session(db_address=db_address) as session: 
        result = session.query(Image)\
            .filter_by(base_filename=base_filename)\
            .one()
        header_exists = result.header is not None
    return header_exists
        

def update_header_data(db_address, base_filename, data_type, header_data):

    # specific header values to add to update columns:
    updates = {
        "header": header_data.get('JSON'),
        "right_ascension": header_data.get('OBJCTRA'),
        "declination": header_data.get('OBJCTDEC'),
        "altitude": header_data.get('ALTITUDE'),
        "azimuth": header_data.get('AZIMUTH'),
        "filter_used": header_data.get('FILTER'),
        "airmass": header_data.get('AIRMASS'),
        "exposure_time": header_data.get('EXPTIME'),
        "user_id": header_data.get('USERID'),
        "username": header_data.get('USERNAME'),
        "data_type": data_type,
    }

    # format capture/sort time as SQL timestamp
    capture_date = re.sub('T', ' ', header_data.get('DATE-OBS'))
    updates["capture_date"] = capture_date
    updates["sort_date"] = capture_date

    with get_session(db_address=db_address) as session:

        # Check if entry for this image already exists
        row_exists = session.query(Image.image_id)\
            .filter_by(base_filename=base_filename)\
            .scalar() is not None

        # If an entry already exists, just update the new bits
        if row_exists:
            item = session.query(Image)\
                .filter(Image.base_filename==base_filename)\
                .update(updates)

        # Create a new row if an entry doesn't exist yet.
        else:
            new_image = Image(
                **updates,
                base_filename=base_filename,
                site=get_site_from_base_filename(base_filename)
            )
            session.add(new_image)
            session.commit()

def update_new_image(db_address:str, base_filename:str, data_type:str, reduction_level:str, filetype:str):

    # Define the 'reduction_level' values that signify different image types
    medium_jpg_reduction_values = ["10", "13"]
    fits_10_reduction_values = ["10", "13"]
    fits_01_reduction_values = ["00", "01"]

    file_exists_column = None 
    if filetype == "jpg" and reduction_level in medium_jpg_reduction_values:
        file_exists_column = "jpg_medium_exists"
    elif filetype == "fits" and reduction_level in fits_01_reduction_values:
            file_exists_column = "fits_01_exists"
    elif filetype == "fits" and reduction_level in fits_10_reduction_values:
            file_exists_column = "fits_10_exists"
    
    if file_exists_column is None:
        print(f"Unknown filetype added: {base_filename}, {data_type}, {reduction_level}, {filetype}")
        return

    updates = {}
    updates[file_exists_column] = True
    updates["data_type"] = data_type


    with get_session(db_address=db_address) as session:

        # Check if entry for this image already exists
        row_exists = session.query(Image.image_id)\
            .filter_by(base_filename=base_filename)\
            .scalar() is not None

        # If an entry already exists, just update the new bits
        if row_exists:
            item = session.query(Image)\
                .filter(Image.base_filename==base_filename)\
                .update(updates)

        # Create a new row if an entry doesn't exist yet.
        else:
            new_image = Image(
                base_filename=base_filename,
                site=get_site_from_base_filename(base_filename),
                **updates
            )
            session.add(new_image)
            session.commit()


def db_remove_base_filename(base_filename):
    """ Remove an entire row represented by the data's base filename.

    Args:
        base_filename (str): identifies what to delete. 
            Example: wmd-ea03-20190621-00000007
    """

    with get_session(db_address=DB_ADDRESS) as session:

        # Identify the row to delete
        row_to_delete = session.query(Image)\
            .filter(Image.base_filename == base_filename)\
            .first()

        # Delete and commit the change
        session.delete(row_to_delete)
        session.commit()

