# aws.py

import boto3
import psycopg2
from botocore.client import Config
import time, re, json
from datetime import datetime


REGION = 'us-east-1'

s3_c = boto3.client('s3', region_name=REGION)
s3_r = boto3.resource('s3', region_name=REGION)


def scan_s3_image_data(bucket, file_prefix='', file_suffix=''):
    print('Searching for data stored in %s bucket...' % bucket)

    data = []
    for key in get_matching_s3_objects(bucket, prefix=file_prefix, suffix=file_suffix): 
        path = key['Key']
        header_file_data = scan_header_file(bucket, path)
        data.append(header_file_data)

    return data


def scan_s3_all_ptr_data(bucket, skip_initial_items=0, prefix='WMD'):
    """
    Get all objects in a given bucket (with optional prefix).

    :param bucket: string name of s3 bucket.
    :param skip_initial_items: number of items to skip when scanning. 
        This will skip the n oldest items in s3. Useful when speed > completeness.
    :param prefix: only collect items with paths starting with this string.
    :return: list of dicts, each dict containing file path and last modified date.
    """

    data = []
    index = 0
    for item in get_matching_s3_objects(bucket, prefix=prefix):
        index += 1  
        file_path = item['Key']

        # psql-formatted datestamp for the object's upload time. Used to sort until header timestamp is available.
        last_modified = datetime.strftime(item['LastModified'], "%Y-%m-%d %H:%M:%S")

        # Skip the first n items
        if index < skip_initial_items: continue 

        data.append({
            'file_path': file_path,
            'last_modified': last_modified,
        })
    
    print(f"Number of items returned: {len(data)}")

    return data


def scan_header_file(bucket, path):
    """
    Create a python dict from a fits-header-formatted text file in s3.

    :param bucket: String name of s3 bucket.
    :param path: String path to a text file in the bucket. 
        note - this file is assumed to be formatted as a fits header.
    :return: dictionary representation the fits header txt file.
    """
    data_entry = {}
    fits_line_length = 80

    contents = read_s3_body(bucket, path)
    
    #print('-> Scanning: ' + path)
    for i in range(0, len(contents), fits_line_length):
        single_header_line = contents[i:i+fits_line_length].decode('utf8')


        # Split header lines according to the FITS header format
        values = re.split('=|/|',single_header_line)
        
        # Get the attribute and value for each line in the header.
        try:

            # The first 8 characters contains the attribute.
            attribute = single_header_line[:8].strip()
            # The rest of the characters contain the value (and sometimes a comment).
            after_attr = single_header_line[10:-1]

            # If the value (and not in a comment) is a string, then 
            # we want the contents inside the single-quotes.
            if "'" in after_attr.split('/')[0]:
                value = after_attr.split("'")[1].strip()
            
            # Otherwise, get the string preceding a comment (comments start with '/')
            else: 
                value = after_attr.split("/")[0].strip()

            # Add the attribute/value to a dict
            data_entry[attribute] = value

            if attribute == 'END': break
        
        except Exception as e:
            print(f"Error with parsing fits header: {e}")

    # Add the JSON representation of the data_entry to itself as the header attribute
    data_entry['JSON'] = json.dumps(data_entry)

    return data_entry


def read_s3_body(bucket_name, object_name):
    s3_object = s3_c.get_object(Bucket=bucket_name, Key=object_name)
    body = s3_object['Body']
    return body.read()


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



if __name__=="__main__":
    print("hello")

    sample_header = "SIMPLE  =                    T                                                  BITPIX  =                   16 /8 unsigned int, 16 & 32 int, -32 & -64 real     NAXIS   =                    2 /number of axes                                  NAXIS1  =                 2098 /fastest changing axis                           NAXIS2  =                 2048 /next to fastest changing axis                   DATE-OBS= '2019-07-10T03:28:55' /YYYY-MM-DDThh:mm:ss observation start, UT      EXPTIME =   3.0000000000000000 /Exposure time in seconds                        EXPOSURE=   3.0000000000000000 /Exposure time in seconds                        SET-TEMP=  -20.000000000000000 /CCD temperature setpoint in C                   CCD-TEMP=  -20.116800000000001 /CCD temperature at start of exposure in C       XPIXSZ  =   13.500000000000000 /Pixel Width in microns (after binning)          YPIXSZ  =   13.500000000000000 /Pixel Height in microns (after binning)         XBINNING=                    1 /Binning factor in width                         YBINNING=                    1 /Binning factor in height                        XORGSUBF=                    0 /Subframe X position in binned pixels            YORGSUBF=                    0 /Subframe Y position in binned pixels            READOUTM= 'Monochrome' /        Readout mode of image                           FILTER  = 'w       ' /          Filter used when taking image                   IMAGETYP= 'Light Frame' /       Type of image                                   FOCALLEN=   2923.1000976562500 /Focal length of telescope in mm                 APTDIA  =   432.00000000000000 /Aperture diameter of telescope in mm            APTAREA =   128618.81743640899 /Aperture area of telescope in mm^2              SBSTDVER= 'SBFITSEXT Version 1.0' /Version of SBFITSEXT standard in effect      SWCREATE= 'MaxIm DL Version 6.20 200613 23VP3' /Name of software                SWSERIAL= '23VP3-SPE3X-YT5E3-3MX1C-3FVM0-CM' /Software serial number            SITELAT = '34 20 35' /          Latitude of the imaging location                SITELONG= '-119 40 52' /        Longitude of the imaging location               JD      =   2458674.6450810186 /Julian Date at start of exposure                JD-HELIO=   2458674.6468506418 /Heliocentric Julian Date at exposure midpoint   OBJECT  = '        '                                                            TELESCOP= 'PlaneWave CDK 432mm' / telescope used to acquire this image          INSTRUME= 'Apogee USB/Net'                                                      OBSERVER= 'WER     '                                                            NOTES   = 'Bring up Images'                                                     FLIPSTAT= '        '                                                            SWOWNER = 'Wayne Rosing' /      Licensed owner of software                      BUNIT   = 'adu     '                                                            IMGCOUNT=                    0                                                  DITHER  =                    0                                                  IMGTYPE = 'Light   '                                                            ENCLOSE = 'Clamshell'                                                           MNT-SIDT=             14.69762                                                  MNT-RA  =            16.331835                                                  MNT-HA  =              25.6342                                                  MNT-DEC =               6.3171                                                  MNRRAVEL=                  0.0                                                  MNTDECVL=                  0.0                                                  AZIMUTH =              135.479                                                  ALTITUDE=               53.976                                                  ZENITH  =               36.024                                                  AIRMASS =                1.236                                                  MNTRDSYS= 'J.now   '                                                            POINTINS= 'tel1    '                                                            MNT-PARK= 'F       '                                                            MNT-SLEW= 'F       '                                                            MNT-TRAK= 'T       '                                                            OTA     = ''                                                                    ROTATOR = ''                                                                    ROTANGLE=                  0.0                                                  ROTMOVNG= 'F       '                                                            FOCUS   = ''                                                                    FOCUSPOS=              11200.0                                                  FOCUSTEM=                 22.1                                                  FOCUSMOV= 'F       '                                                            WX      = ''                                                                    SKY-TEMP=                -21.8                                                  AIR-TEMP=                 17.9                                                  HUMIDITY=                 76.0                                                  DEWPOINT=                 13.6                                                  WIND    =                  0.0                                                  PRESSURE=                970.0                                                  CALC-LUX=              171.513                                                  SKY-HZ  =               6208.0                                                  DETECTOR= ''                                                                    CAMNAME = 'ea03    '                                                            GAIN    =                 1.18                                                  RDNOISE =                 5.82                                                  PIXSCALE=                 0.95                                                  FILEPATH= 'Q:\archive\ea03\'                                                    FILENAME= 'WMD-ea03-20190709-00000685-E00.fits'                                 END                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             "
    bucket = 'photonranch-001'

    scan_s3_all_ptr_data(bucket)
