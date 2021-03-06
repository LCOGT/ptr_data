# psql.py

"""

TODO: create a method that will scan the database for entries, compare them
      with the items in s3, and apply any updates (rather than an entire
      delete + insert sequence).

TODO: remove hardcoded site, created_user values.
TODO: if right_ascension is presented in sexagesimal format, convert to decimal rather than skipping

"""

from utils import aws
import boto3
import psycopg2
import re
from datetime import datetime
import json
from tqdm import tqdm
from PyAstronomy import pyasl



def delete_all_entries(cursor, connection):
    '''
    Clear the database of all entries. 
    '''

    cursor.execute("DELETE FROM images") 
    cursor.execute("DELETE FROM users")
    connection.commit()
    print('\n{:^80}\n'.format('**DATABASE IS EMPTY**'))


def insert_all_entries(cursor, connection, bucket):
    items = aws.scan_s3_all_ptr_data(bucket, 0, 'wmd')

    #place a single hard-coded user into the users table under which all images are referenced
    username = 'wmd_admin'
    sql = ("INSERT INTO users("
        "user_name) "
        "VALUES (%s) "
        "RETURNING user_id"
    )
    cursor.execute(sql,(username,))
    created_user = cursor.fetchone()

    for item in tqdm(items): 
        file_path = item['file_path']

        # convert date format (Fri, 21 Jun 2019 20:23:02 GMT) to (2019-06-21 20:23:02)
        last_modified = item['last_modified'] 
        #last_modified_formatted = datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z')

        # The 'filename' that looks somethign like 'WMD-ea03-20190621-00000007-E00.fits.bz2'
        file_key = file_path.split('/')[-1]
        
        # The base_filename (aka primary key) is something like 'WMD-ea03-20190621-00000007'
        base_filename = file_key[:26]
        
        # The data_type is the 'E00' string after the base_filename.
        data_type = file_key[27:31]
        
        # The file_extension signifies the filetype, such as 'fits' or 'txt'.
        file_extension = file_key.split('.')[1]
        
        # The site is derived from the beginning of the base filename (eg. 'WMD')
        site = base_filename[0:3] 
    


        # This parameter is set to true when an object can write a valid database entry.
        valid_sql_to_execute = False
        items_not_added = 0


        # Handle text file (which stores the fits header data)
        if file_extension == "txt":
            
            header_data = aws.scan_header_file(bucket, file_path)
            sql = ("INSERT INTO images("

                   "base_filename, "
                   "created_user, "
                   "site, "
                   "capture_date, "
                   "sort_date, "
                   "right_ascension, "
                   "declination, "
                   "altitude, "
                   "azimuth, "
                   "filter_used, "
                   "airmass, "
                   "exposure_time, "
                   "header) "

                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                   "ON CONFLICT (base_filename) DO UPDATE SET "

                   "created_user = excluded.created_user, "
                   "capture_date = excluded.capture_date, "
                   "sort_date = excluded.sort_date, "
                   "right_ascension = excluded.right_ascension, "
                   "declination = excluded.declination, "
                   "altitude = excluded.altitude, "
                   "azimuth = excluded.azimuth, "
                   "filter_used = excluded.filter_used, "
                   "airmass = excluded.airmass, "
                   "exposure_time = excluded.exposure_time, "
                   "header = excluded.header;"
            )

            # extract values from header data
            capture_date = header_data.get('DATE-OBS')
            header = header_data.get('JSON')
            right_ascension = header_data.get('OBJCTRA')
            declination = header_data.get('OBJCTDEC')
            altitude = header_data.get('ALTITUDE')
            azimuth = header_data.get('AZIMUTH')
            filter_used = header_data.get('FILTER')
            airmass = header_data.get('AIRMASS')
            exposure_time = header_data.get('EXPTIME')
            
            # in case the fits header does not have a capture time stored in it
            try:
                capture_date = re.sub('T', ' ', capture_date) # format capture time as SQL timestamp
                sort_date = capture_date #set this if we have a valid capture time
            except:
                capture_date = None
                sort_date = last_modified #set this if we don't have a valid capture time

            # Check that RA and DEC are in deg format, not sexagesimal
            print(right_ascension)
            if right_ascension is not None and "." in right_ascension:
                pass
            else:
                right_ascension = None
                declination = None
            
            # These values will be fed into the sql command string (above)
            attribute_values = [
                base_filename,
                created_user,
                site,
                capture_date,
                sort_date, 
                right_ascension,
                declination,
                altitude,
                azimuth,
                filter_used,
                airmass,
                exposure_time,
                header
            ]

            valid_sql_to_execute = True


        # Handle non-text files (eg. fits or jpg)
        elif file_extension in ['fits', 'jpg']:
            
            # Define the attribue column we will set to true.
            file_exists_attribute = f"{data_type.lower()}_{file_extension}_exists"

            # Create a new element with the primary key, site, and file_exists_attribue=true. 
            # If there is already an element with this primary key, update the state of file_exists_attribute = true
            # TODO: rewrite to avoid injection vulnerability. Risk is lower because site code automatically controls the filenames, but still not good.
        
            sql = (f"INSERT INTO images (base_filename, site, sort_date, {file_exists_attribute}) "
                    "VALUES(%s, %s, %s, %s) ON CONFLICT (base_filename) DO UPDATE "
                    f"SET {file_exists_attribute} = excluded.{file_exists_attribute};"
            )
            
            # These values will be fed into the sql command string (above)
            attribute_values = (
                base_filename, 
                site, 
                last_modified, 
                True
            )

            valid_sql_to_execute = True

        else:
            print(f"[psql.py > insert_all_entries] Unrecognized file type: {file_extension}. Skipping file.")
            items_not_added += 1

        # Execute the sql if it has been properly created.
        if valid_sql_to_execute: 
            cursor.execute(sql,attribute_values)
            # try:
            #     cursor.execute(sql,attribute_values)
            # except:
            #     print(f"There was an error when reading {base_filename}. RA, DEC: {right_ascension}, {declination}")

    connection.commit()
    print("")
    print(f"Successfully added {len(items)-items_not_added} entries to the database.")
    print(f"Failed to add {items_not_added} items.")




def get_last_modified(cursor, connection, k):
    sql = "SELECT base_filename FROM images ORDER BY capture_date DESC LIMIT %d" % k
    try:
        cursor.execute(sql)
        images = cursor.fetchmany(k)
    except (Exception, psycopg2.Error) as error :
        print("Error while retrieving records:", error)

    return images



