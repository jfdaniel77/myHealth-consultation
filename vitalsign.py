import boto3
import pymongo
import paho.mqtt.client as mqtt

from time import time
from json import dumps, loads
from datetime import datetime, date


# Constants
LOGPREFIX = "myHealth"

# Vital Sign consumer
def consume_vital_sign(event, context):
    print("{} - Consume Vital Sign".format(LOGPREFIX))
    
    # Consume data from straming queue
    list_records = []
    
    for record in list_records:
        store_data(record)
    
    return {"statusCode": 200}

# Submit Vital Sign
def submit_vital_sign(event, context):
    print("{} - Submit Vital Sign".format(LOGPREFIX))
    user_id = ''
    response_payload = {}
    
    # Get input parameter
    if event.get("pathParameters") and event.get("pathParameters").get("userid"):
        user_id = event.get("pathParameters").get("userid")
    else:
        return {"statusCode": 400, "body": "Missing User ID", "headers": {"Content-Type": "text/plain"}}
        
    payload = loads(event.get("body"))
    print("{} - id: {} and payload: {}".format(LOGPREFIX, user_id, payload))
    
    if payload:
        response_payload = store_data(payload)
    else:
        return {"statusCode": 400, "body": "Missing Payload", "headers": {"Content-Type": "text/plain"}}
    
    return {"statusCode": 200, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
def get_parameter_value(key):
    print("{} - Get Parameter key: {}".format(LOGPREFIX, key))
    ssm_client = boto3.client("ssm")
    value = ssm_client.get_parameter(Name=key, WithDecryption=False)
    return value.get("Parameter").get("Value")

def store_data(payload):
    print("{} - Store Data to MongoDB".format(LOGPREFIX))
    
    response_payload = {}
    
    mongodb_url = get_parameter_value('serverless-mongodb-url')
    mdb_client = pymongo.MongoClient(mongodb_url)
    db = mdb_client['myhealth']
    vitalsign = db['vitalsign']
    vitalsign_id = vitalsign.insert_one(payload)
    
    response_payload = {
        "id": str(vitalsign_id.inserted_id)
    }
    
    return response_payload