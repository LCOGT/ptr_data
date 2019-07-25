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
In order to run and test the database script, first find the .config file in the repo and change/fill out its fields accordingly.
```
DB_USER = username for database access
DB_USER_PASS = password for database access

DB_HOST = location of remote database instance to connect to
DB_IDENTIFIER = name of database instance on rds
DB = name of database to access
```
Values for username and password for database access can be found on the LCOGT information spreadsheet.
