import boto3
import json

def get_queue_url(queueName):
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.get_queue_url(
        QueueName=queueName,
    )
    return response["QueueUrl"]

def send_to_datastream(site, data):
    sqs = boto3.client('sqs')
    queue_url = get_queue_url('datastreamIncomingQueue-dev')

    payload = {
        "topic": "imagedata",
        "site": site,
        "data": data,
    }
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(payload),
    )
    return response