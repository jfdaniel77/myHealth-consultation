import pymongo
import boto3

from json import dumps, loads


def hello(event, context):
    body = {
        "message": "Welcome to myHealth Service. It is built using Serverless v2.0! Your function executed successfully!"
    }

    response = {"statusCode": 200, "body": dumps(body)}

    return response

    # Use this code if you don't use the http event with the LAMBDA-PROXY
    # integration
    """
    return {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "event": event
    }
    """

def submit_vital_sign(event, context):
    
    user_id = ''
    
    # Get input parameter
    if event.get("pathParameters") and event.get("pathParameters").get("id"):
        user_id = event.get("pathParameters").get("id")
    else:
        return {"statusCode": 400, "body": "Missing User ID", "headers": {"Content-Type": "text/plain"}}
        
    payload = loads(event.get("body"))
    
    
        
    body = {
        "message": "Submit Vital Sign for {}".format(user_id),
        "payload": dumps(payload)
    }
        
    response = {"statusCode": 200, "body": dumps(body), "headers": {"Content-Type": "application/json"}}
    
    return response