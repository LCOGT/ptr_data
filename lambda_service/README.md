# ptrdata runs on aws lambda

This directory contains the code and scripts needed to deploy the code on aws lambda. We use the serverless framework for deployment. 

## Initialization:

Make sure you have amazon iam credentials saved on your machine. 

From this directory (ptrdata), run `npm install` to get the required serveless dependencies. 

Rename `default.env` to `.env` and fill out the empty values. These define the environment variables used to connect to the database. 

Run `$ serverless` to confirm that the serverless project is recognized. Answer 'no' if asked to connect the service to a free serverless account.

## Usage:

The lambda function that updates our database is in `insert_data.py`, and runs on python3.7. To change the code, modify this file, and then run `$ serverless deploy`.  

## Details:

The serverless framework zips the contents of its directory and uploads them to a specified bucket in s3. The lambda function itself pulls its updates from this bucket. 

## Removal:

You can remove all associated aws resources (lambda function, s3 bucket, iam roles, cloudformation stack, etc.) with `$ serverless remove`. Everything can be restored just as easily with `$ serverless deploy`. Be careful that nothing is saved in a way that will be lost by running these two commands.

