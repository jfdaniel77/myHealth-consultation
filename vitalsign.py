import boto3
import pymongo

from time import time
from json import dumps, loads
from datetime import datetime, date


# Constants
LOGPREFIX = "myHealth"

# Vital Sign consumer
def consume_vital_sign(event, context):
    print("{} - Consume Vital Sign".format(LOGPREFIX))
    
    # Get Input Parameter
    user_id = None
    if event.get("pathParameters") and event.get("pathParameters").get("userId"):
        user_id = event.get("pathParameters").get("userId")
    else:
        response_payload = {
            "message": "Missing User ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    print("{} - User ID: {}".format(LOGPREFIX, user_id))
    
    start_dt_str = None
    if event.get("queryStringParameters") and event.get("queryStringParameters").get("start"):
        start_dt_str = event.get("queryStringParameters").get("start")
    else:
        response_payload = {
            "message": "Missing Start Date"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    print("{} - Start Date: {}".format(LOGPREFIX, start_dt_str))
    
    end_dt_str = None
    if event.get("queryStringParameters") and event.get("queryStringParameters").get("end"):
        end_dt_str = event.get("queryStringParameters").get("end")
    else:
        response_payload = {
            "message": "Missing End Date"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    print("{} - End Date: {}".format(LOGPREFIX, end_dt_str))
    
    # Prepare payload
    start_timestamp = int(getTimestamp(datetime.strptime(start_dt_str, "%d/%m/%Y")))
    end_timestamp = int(getTimestamp(datetime.strptime(end_dt_str, "%d/%m/%Y")))
    
    payload = {
        "USER_ID": user_id,
        "datetime": {"$gte": start_timestamp, "$lte": end_timestamp}
    }
    
    # Consume data from straming queue
    list_records = get_data(payload)
        
    print("{} - List Vital Sign Record: {}".format(LOGPREFIX, list_records))
    return {"statusCode": 200, "body": dumps(list_records, default=str), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}

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
        payload["USER_ID"]: user_id
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
    
def get_data(payload):
    print("{} - Get Data from MongoDB".format(LOGPREFIX))
    print("{} - Payload: {}".format(LOGPREFIX, payload))
    
    list_records = []
    
    mongodb_url = get_parameter_value('serverless-mongodb-url')
    mdb_client = pymongo.MongoClient(mongodb_url)
    db = mdb_client['myhealth']
    vitalsign = db['vitalsign']
    
    mdb_cursor = vitalsign.find(payload, {"_id": 0}).sort("datetime")

    for item in mdb_cursor:
        list_records.append(item)
    
    return list_records
    
def getTimestamp(value):
    return value.timestamp() * 1000
