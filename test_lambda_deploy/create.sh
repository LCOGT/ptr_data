cd lambda
zip ../function.zip TimTestAutoDeploy.py
cd ..

aws lambda create-function --function-name TimTestAutoDeploy \
--zip-file fileb://function.zip \
--handler TimTestAutoDeploy.my_handler \
--runtime python3.6 \
--role arn:aws:iam::306389350997:role/lambda-vpc-role

rm function.zip
