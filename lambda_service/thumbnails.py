import os
import boto3
import sys
import uuid
from urllib.parse import unquote_plus
import PIL
from PIL import Image


s3_client = boto3.client('s3')

def resize_image(image_path, resized_path, height_pix):
    with Image.open(image_path) as image:

        width, height = image.size
        scale_factor = height / height_pix
        resize_dimensions = (int(width / scale_factor), height_pix)
        
        image.thumbnail(resize_dimensions)
        image.save(resized_path)


def resize_handler(bucket, key, thumbnail_key, height_pix):
    tmpkey = key.replace('/', '')
    download_path = f"/tmp/{uuid.uuid4()}{tmpkey}"
    upload_path = f"/tmp/resized-{tmpkey}"

    s3_client.download_file(bucket, key, download_path)
    resize_image(download_path, upload_path, height_pix)
    s3_client.upload_file(upload_path, bucket, thumbnail_key)