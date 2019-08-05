# AWS Lambda Code

### important note: this code is copied and pasted from the aws lambda console. It is not necessarily up to date. 

This directory contains two files. 
- `insert_data.py` is the code run as a lambda function. It is responsible for updating the database every time new data arrives in s3. 
- `lambda_tests.txt` is a file containing the tests that can be configured and run in the lambda web console. Since it doesn't seem possible to save and share tests from the lambda console, this is our workaround. To use them, create a new test configuration from the lambda console, and paste in a configuration from `lambda_tests.txt`.
