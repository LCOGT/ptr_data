import numpy as np
import time
import bz2
import os
import datetime
import random
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

