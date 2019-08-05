# ptr_data

This repo contains Photon Ranch's interface for managing a PostgreSQL database used for storing user image data. This script connects to a remote database instance on Amazon RDS and submits SQL queries through a python application using the psycopg2 module. As images are taken and stored on S3, this script will scan and insert new image meta-data into the database. 

## Getting Started

These instructions will get you a copy of the database script up and running on your local machine for use and development.

### Setting up the code
To begin working, first acquire the ptr_api repository from github and set up a Python virtual environment.

##### Clone the repo
```bash
$ git clone https://github.com/LCOGT/ptr_data.git
$ cd ptr_data
```

##### Create Python virtual environment - version 3.6 or higher
```bash
$ python3.6 -m venv venv
```

##### Activate the virtual environment
```bash
$ source venv/bin/activate
```

### Install dependencies
Use the python package-management system in order to install required modules within the virtual environment:
```bash
(venv)$ pip install -r requirements.txt   
```

### Populate required config files
In order to run and test the database script, first find the database.ini-MODIFYME file in the repo and rename it 'database.ini. Change/fill out its fields accordingly:
```
[postgresql]
host = location of remote database instance on rds
database = name of ptr database
user = username for database access
password = password for database access

[aws]
db_identifier = name of database instance on rds
bucket = name of bucket containing ptr images
region = region of aws resources
```
Values for username and password for database access can be found on the LCOGT information spreadsheet.

## Populating PTR Archive
In order to scan s3 and update all entries in the database simply run:
```bash
$ python database.py
```
This will delete all current entries in the database and repopulate records based on the files found in the bucket specified within the .ini config file.
