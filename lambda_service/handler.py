import boto3
import json
import logging
import time
import os

logger = logging.getLogger("handler_logger")
logger.setLevel(logging.DEBUG)

dynamodb = boto3.resource("dynamodb", region_name=os.getenv('REGION'))

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


########################################
##### Send to subscribers methods ######
########################################

def _send_to_connection(gatewayClient, connection_id, data, wss_url):
    return gatewayClient.post_to_connection(
        ConnectionId=connection_id,
        Data=json.dumps({"messages":[{"content": data}]}).encode('utf-8')
    )


def sendToSubscribers(data):
    wss_url = os.getenv('WSS_URL')
    gatewayApi = boto3.client("apigatewaymanagementapi", endpoint_url=wss_url)

    # Get all current connections
    table = dynamodb.Table(SUBSCRIBERS_TABLE)
    response = table.scan(ProjectionExpression="ConnectionID")
    items = response.get("Items", [])
    connections = [x["ConnectionID"] for x in items if "ConnectionID" in x]

    # Send the message data to all connections
    logger.debug("Broadcasting message: {}".format(data))
    dataToSend = {"messages": [data]}
    for connectionID in connections:
        try: 
            connectionResponse = _send_to_connection(
                    gatewayApi, connectionID, dataToSend, os.getenv('WSS_URL'))
            logger.info((
                f"connection response: "
                f"{json.dumps(connectionResponse)}")
            )
        except gatewayApi.exceptions.GoneException:
            error_msg = (
                f"Failed sending to {connectionID} due to GoneException. "
                "Removing it from the connections table."
            )
            logger.exception(error_msg)
            _remove_connection(connectionID)
            continue
        except Exception as e:
            print(e)
            continue