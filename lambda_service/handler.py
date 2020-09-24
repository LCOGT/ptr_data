import boto3
import json
import logging
import time
import os

logger = logging.getLogger("handler_logger")
logger.setLevel(logging.DEBUG)

dynamodb = boto3.resource("dynamodb")

"""
TODO:

1. Retrieve table name from env variable instead of hardcode.

2. Clean/Refactor

"""
SUBSCRIBERS_TABLE = os.getenv('SUBSCRIBERS_TABLE')

def _get_response(status_code, body):
    if not isinstance(body, str):
        body = json.dumps(body)
    return {"statusCode": status_code, "body": body}

def connection_manager(event, context):
    """
    Handles connecting and disconnecting for the Websocket
    """
    connectionID = event["requestContext"].get("connectionId")


    if event["requestContext"]["eventType"] == "CONNECT":
        logger.info("Connect requested")

        # Add connectionID to the database
        table = dynamodb.Table(SUBSCRIBERS_TABLE)
        table.put_item(Item={
            "ConnectionID": connectionID, 
            #"Observatory": observatory
        })
        return _get_response(200, "Connect successful.")

    elif event["requestContext"]["eventType"] in ("DISCONNECT", "CLOSE"):
        logger.info("Disconnect requested")
        
        # Remove the connectionID from the database
        _remove_connection(connectionID)
        #table = dynamodb.Table("photonranch-data-subscribers1")
        #table.delete_item(Key={
            #"ConnectionID": connectionID
            ##"Observatory": observatory
        #}) 

        return _get_response(200, "Disconnect successful.")



    else:
        logger.error("Connection manager received unrecognized eventType '{}'")
        return _get_response(500, "Unrecognized eventType.")

def _remove_connection(connectionID):
    table = dynamodb.Table(SUBSCRIBERS_TABLE)
    response = table.delete_item(Key={
        "ConnectionID": connectionID
    })
    return response