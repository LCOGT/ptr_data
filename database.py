# database.py

from utils import helpers
from utils import psql
import psycopg2
import time, re
import boto3
from dotenv import load_dotenv
import os
from os.path import join, dirname

REGION = 'us-east-1'
BUCKET_NAME = 'photonranch-001'

dotenv_path = join(dirname(__file__), '.config')
load_dotenv(dotenv_path)

DB_USER = os.environ.get('DB_USER')
DB_USER_PASS = os.environ.get('DB_USER_PASS')

DB_HOST = os.environ.get('DB_HOST')
DB_IDENTIFIER = os.environ.get('DB_IDENTIFIER')
DB = os.environ.get('DB')

print('\n' + '~~~~~~~~~~CONNECTING TO YOUR AWS-RDS DATABASE INSTANCES~~~~~~~~~~')
psql.db_connect(DB_IDENTIFIER, DB, DB_USER, DB_USER_PASS, DB_HOST)

