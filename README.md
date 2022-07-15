# Data Archive for Photon Ranch

This is a serverless application that handles new images from observatories.

New images arriving in S3 automatically trigger a routine (defined in this repository) that updates the postgres
database with metadata, and notifies the frontend that a new image exists using the datastreamer service.

## Data Files in Photon Ranch

Each exposure generates a number of different files. The types, with example filenames, are included below:

- Text file: `sro-kb001ms-20220629-00010442-EX01.txt`
  - Contains the entire fits header, used for faster ingestion before the fits data arrives
- Large fits: `sro-kb001ms-20220629-00010442-EX01.fits.bz2`
- Small fits: `sro-kb001ms-20220629-00010442-EX10.fits.bz2`
- Medium JPG: `sro-kb001ms-20220629-00010442-EX10.jpg`
- Thumbnail JPG: `sro-kb001ms-20220629-00010442-EX11.jpg`
  - The thumbnail jpg is generated in AWS from the medium jpg.

All of these files have similar filenames, with the differentiating bits at the end of the filename. The part that is
shared by all files is called the base filename.

The base filename is constructed using the format {site}-{instrument}-{YYYYMMDD}-{8 digit incrementing number}. In the
examples above, the base filename would be: `sro-kb001ms-20220629-00010442`.

Data files can be set to expire after a period of time. Expirations are handled in the dynamodb table
`data-expiration-tracker`, which will remove entries from s3 and the postgres database. 

## Types of images

There are three types of images: regular exposures (the most common), info images, and allsky images.
There is a simple scheme for differentiating them all in the frontend. Objects in s3 that belong to regular exposures
are prefixed by `data/`, info images are prefixed with `info-images/`, and allsky images (when they are implemented at
sites) will be prefixed by `allsky/`.

### Regular Exposures (data/)

Example jpg object in s3: `data/sro-kb001ms-20220629-00010442-EX10.jpg`

### Info Images (info-images/)

Example jpg object in s3: `info-images/sro-kb001ms-20220629-00010442-EX10.jpg`

Info images are temporary images that are sticked in front of the row of image thumbnails. The purpose is to provide
a simple way for observatory sites to send useful visual data to the user. This might include color stacks, in-progress
mosaics, or even plots and graphs related to some ongoing activity.

Info images can be identical in format to regular exposures, with large and small fits files, jpgs, and metadata in a
text file. If fits files are present, users can run analysis tasks on them just like normal images, and if metadata
exists, it will be viewable with the 'show fits header' button. At the same time, like normal images, info images can be
as sparse as a single jpg and still show up in the frontend.

Info images are automatically set to be deleted after 48 hours.

There are three channels for info images. Each channel is a sticky space that sits left of the regular image thumbnails.
This means up to three info images can be visible at a site at the same time. A new info image to a channel with an
existing image will overwrite the value with the new one. Each channel handles expirations separately too.

Info images are not part of the postgres database that tracks regular exposures. They are handled instead in the
dynamodb table called `info-images`.

Below is a screenshot from the tst site [(this page)](https://www.photonranch.org/site/tst/observe) showing where info
images appear in the frontend. You can test out the functionality for yourself by uploading them manually using the
upload script in [photonranch-helper-scripts](https://github.com/LCOGT/photonranch-helper-scripts).

![Info Image Example](img/Info%20Image%20Example.png)

### All-sky Images (allsky/)

Example jpg object in s3: `allsky/sro-kb001ms-20220629-00010442-EX10.jpg`

All sky cameras have not yet been added to any observatory sites. However, the loose plan is to track them separately
from regular exposures and info images, with objects stored using the separate `allsky/` prefix.

## Architecture

The bulk of our image data (the regular exposures) are maintained with two storage systems. An S3 bucket stores all the
files, and a postgres database stores pointers to objects in S3, along with metadata used to efficiently organize and
query the data.

The s3 bucket currently in use is `photonranch-001`. The bucket settings include important lifecycle rules in the
bucket management tab, including:

- moving 30-day-old images to infrequent access for cheaper storage
- remove info images objects after 48 hours
- remove tif files from downloads cache after 48 hours

### S3 Logging

As a tool for smoother debugging, all objects that arrive in s3 are also logged in the dynamodb table
`recent-uploads-log`. They are visible on the Photon Ranch frontend at www.photonranch.org/site/{site}/observe, under
the dev tools tab. Currently, logs persist indefinitely. However, it would make sense to eventually add a time-to-live
(ttl) attribute to these entries so that the dynamodb table does not grow unconstrained.

### Postgres Database

The postgres database is run using AWS RDS. Local inspection and schema modification is possible using tools like pgAdmin. Credentials for
connecting to the database can be found in the PTR System Information spreadsheet in the shared Photon Ranch Drive.

If the database schema is modified, the models in db.py will need to be updated in this repository and in photonranch-api.

The postgres table Images contains the following columns:

- image_id (integer): incrementing primary key used to identify an exposure.
- base_filename (character varying 29): part of the filename formatted {site}-{instrument}-{YYYYMMDD}-{8 digit incrementing number}
- site(character varying 6): the short name of a site, such as mrc.
- capture_date(timestamp without timezone): time of image capture
- sort_date (timestamp without timezone): same as capture_date, exists for legacy reasons.
- right_ascension (double precision): right ascension in decimal hours
- declination (double precision): declinatino in decimal degrees
- altitude (double precision): altitude in decimal degrees
- azimuth (double precision): azimuth in decimal degrees
- header (character varying): the entire fits header as a json string
- filter_used (character varying): name of the imaging filter used in the exposure
- airmass (double precision): airmass of the exposed target
- user_id (character varying): Unique ID from the Auth0 account that requested the image. Also accessed as the "sub" value in the frontend.
- username (character varying): username for the Auth0 account that requested the image.
- fits_01_exists (boolean): true iff the large fits file exists. Applies to fits files with reduction values of 00 and 01.
- fits_10_exists (boolean): true iff the small fits file exists. Applies to fits files with reduction values of 10 and 13.
- jpg_medium_exists (boolean): true iff the medium jpg exists. This applies to jpgs with the reduction value of 10 and 13.
- jpg_small_exists (boolean): true iff the thumbnail jpg exists. This is a jpg with a reduction value of 11.
- data_type (boolean): the data type stores the 2-character value before the reduction level in the filename. Commonly EX or e.

Database queries can use any of these attributes to refine the query results. Since this repository does not expose a
public facing API, queries from external services happen through photonranch-api instead of this repository.
### Path of a file

To step through this process yourself, try uploading files to the tst site with the upload script in
[photonranch-helper-scripts](https://github.com/LCOGT/photonranch-helper-scripts).

![Path of a file](img/Path%20of%20a%20File.jpeg)

This chart can be modified [here](https://lucid.app/lucidchart/187b7f90-7b61-4c01-85a9-727e149de244/edit?viewport_loc=-1874%2C-40%2C2614%2C1482%2C0_0&invitationId=inv_10c7330d-bc73-44a3-b11c-9b7275be95c1#).

## Testing and Deployment

Unit test and integration tests are defined in the tests folder, and configured in `pytest.ini`.
Both can be run with pytest. Create a virtual environment using python3.7 or later, and install the dependencies in
`requirements.txt`. Finally, run the tests with `$ pytest` from the root directory.

Integration tests use the production S3 bucket, but rely on simulation data prefixed with `test/`. In the s3 console,
this is the data under the 'folder' called `test`.

This app is deployed using the serverless framework. There is currently only one environment, so simply run
`serverless deploy` from your local machine to deploy any changes.

## Improvements

- Most if not all of this code will be replaced by a migration to the
  [OCS Science Archive](https://github.com/observatorycontrolsystem/science-archive).
- The contents of db.py in this repository and in photonranch-api have a lot of repeated code. It would make sense to
  merge the contents of these repositories, especially since photonranch-api houses the endpoints used to upload and
  download images.
- The fits header info is first loaded into the postgres database from a text file uploaded to s3. It would make more
  sense to have that info included in the http request body used to get the upload url (in photonranch-api). The reason
  we use a text file for the fits header currently is just so that information arrives before the fits files are
  uploaded. One approach to eliminating the need for the text file:
  - Add the header data in the photonranch-api /upload POST
  - The handler for the upload endpoint, in addition to its current behavior, should create or update the entry in the
   postgres database for the new file, if an entry doesn't already exist.
  - The files uploaded to s3 should trigger the same lambda-based functionality that uploads the postgres database with
    any additional metadata.
- Create a separate prod, dev, and test deployment. This will involve creating separate S3 buckets manually, since they
  cannot be managed as resources within Serverless.
