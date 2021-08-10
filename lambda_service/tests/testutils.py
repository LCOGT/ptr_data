import numpy as np
import json
import requests
import bz2
import os
import datetime
import random
import time
from astropy.io import fits
from PIL import Image


def to_bz2(filename):
    with open(filename, 'rb') as uncomp:
        comp = bz2.compress(uncomp.read())
    with open(filename + '.bz2', 'wb') as target:
        target.write(comp)


def make_header(extras={}):
    header_dict = {
        "DATE-OBS": datetime.datetime.now().isoformat().split('.')[0],
        "IMAGETYP": "TEST DATA",
        "OBJCTRA": random.random()*24,
        "OBJCTDEC": (random.random() * 180) - 90,
        "ALTITUDE": round(random.random() * 60 + 30, 3),
        "AZIMUTH": round(random.random() * 180, 3),
        "FILTER": "'L'",
        "AIRMASS": round(random.random() * 2 + 1, 3),
        "EXPTIME": round(random.randint(1, 20), 3),
        "USERID": "'google-oauth2|100354044221813550027'",
        "USERNAME": "'Tim Beccue'",
        "HEADER": "'header'",
        **extras
    }
    return header_dict


def make_data_files(data=None, header_dict={}, savedirectory="lambda_service/tests/testing_data"):
    print(os.getcwd())
    filename = f"{savedirectory}/testdata.fits"

    # simple default array if none was provided
    if data is None:
        data = np.array([
            np.arange(0,4),
            np.arange(1,5),
            np.arange(2,6),
            np.arange(3,7),
        ])

    hdu = fits.PrimaryHDU()
    hdu.data = data

    # add header to fits data
    for key in header_dict:
        hdu.header[key] = header_dict[key]

    # write header text file
    with open(f'{savedirectory}/testdata.txt', 'w') as txt:
        txt.write(str(hdu.header))

    # write jpg file
    positive_jpg_data = hdu.data + \
        np.abs(np.amin(hdu.data))  # make all vals positive
    #jpg_data = Stretch().stretch(positive_jpg_data)  # stretch
    jpg_data = positive_jpg_data
    jpg_8 = (jpg_data * 256).astype('uint8')  # convert to 8-bit int for jpg
    im = Image.fromarray(jpg_8)
    im.save(f"{savedirectory}/testdata.jpg")

    # write fits file
    hdu.writeto(filename, overwrite=True)

    # write bz2 fits file
    to_bz2(filename)


def get_upload_url(filename, s3_directory, info_channel=None):
    request_body = {
        "object_name": filename,
        "s3_directory": s3_directory,
    }
    if info_channel is not None:
        request_body['info_channel'] = info_channel
    url = "https://api.photonranch.org/test/upload"
    response = requests.post(url, json.dumps(request_body))
    return response.json()


def upload_files(filename_list, s3dir, info_channel=None):
    for f in filename_list:
        upload_url = get_upload_url(f[0], s3dir, info_channel)
        with open(f[1], 'rb') as base_file:
            data = {'file': (f[0], base_file)}
            upload_response = requests.post(
                upload_url["url"], upload_url["fields"], files=data)
            print(f'uploading {upload_url["fields"]["key"]}')
            print(upload_response)


def get_s3_event(key):
    """ simulate the event that is provided to the lambda handler when s3 detects a new item """
    return {
        'Records': [
            {
                's3': {
                    'bucket': { 'name': 'photonranch-001' },
                    'object': { 'key': key }
                }
            }
        ]
    }