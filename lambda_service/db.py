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

from lambda_service.helpers import get_s3_image_path, get_s3_file_url

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

Base = declarative_base()
DB_ADDRESS = os.getenv('DB_ADDRESS')


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


class Image(Base):
    __tablename__ = 'images'

    image_id         = Column(Integer, primary_key=True)
    base_filename    = Column(String)
    site             = Column(String)
    capture_date     = Column(DateTime, default=datetime.utcnow)
    sort_date        = Column(DateTime, default=datetime.utcnow)
    right_ascension  = Column(Float)
    declination      = Column(Float)
    ex00_fits_exists = Column(Boolean)
    ex01_fits_exists = Column(Boolean)
    ex10_fits_exists = Column(Boolean)
    ex13_fits_exists = Column(Boolean)
    ex10_jpg_exists  = Column(Boolean)
    ex13_jpg_exists  = Column(Boolean)
    altitude         = Column(Float)
    azimuth          = Column(Float)
    filter_used      = Column(String)
    airmass          = Column(Float)
    exposure_time    = Column(Float)
    username         = Column(String)
    user_id          = Column(String)
    header           = Column(String)

    def __init__(self, **kwargs):
        self.base_filename = base_filename

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
            "site": self.site, 

            "exposure_time": self.exposure_time,
            "filter_used": self.filter_used,
            "right_ascension": self.right_ascension, 
            "declination": self.declination, 
            "azimuth": self.azimuth,
            "altitude": self.altitude,
            "airmass": self.airmass,

            "ex01_fits_exists": self.ex01_fits_exists,
            "ex10_fits_exists": self.ex10_fits_exists,
            "ex10_jpg_exists": self.ex10_jpg_exists,
            "ex13_jpg_exists": self.ex13_jpg_exists,
            "ex00_fits_exists": self.ex00_fits_exists,

            "username": self.username,
            "user_id": self.user_id,
        }

        # Convert to timestamp in milliseconds
        package["capture_date"] = int(1000 * self.capture_date.timestamp())
        package["sort_date"] = int(1000 * self.sort_date.timestamp())

        # Include a url to the jpg
        package["jpg_url"] = ""
        if self.ex10_jpg_exists:
            path = get_s3_image_path(self.base_filename, "EX10", "jpg")
            url = get_s3_file_url(path)
            package["jpg_url"] = url

        return package



def update_header_data(db_address, base_filename, header_data):

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
    }

    # format capture/sort time as SQL timestamp
    capture_date = re.sub('T', ' ', header_data.get('DATE-OBS'))
    updates["capture_date"] = capture_date
    updates["sort_date"] = capture_date

    with get_session(db_address=db_address) as session:

        # Check if entry for this image already exists
        row_exists = session.query(Image.image_id)\
            .filter_by(base_filename=base_filename)\
            .salar() is not None

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
                site=base_filename[0:3]
            )
            session.add(new_image)
            session.commit()

def update_new_image(db_address, base_filename, exversion, file_extension):

    file_exists_attribute = f"{exversion.lower()}_{file_extension}_exists"

    with get_session(db_address=db_address) as session:

        # Check if entry for this image already exists
        row_exists = session.query(Image.image_id)\
            .filter_by(base_filename=base_filename)\
            .salar() is not None

        # If an entry already exists, just update the new bits
        if row_exists:
            item = session.query(Image)\
                .filter(Image.base_filename==base_filename)\
                .update(Image[file_exists_attribute]=True)

        # Create a new row if an entry doesn't exist yet.
        else:
            new_image = Image(
                base_filename=base_filename,
                site=base_filename[0:3],
                Image[file_exists_attribute]=True
            )
            session.add(new_image)
            session.commit()


