import pymssql
import boto3

from sqlalchemy.orm import declarative_base
from sqlalchemy.schema import Sequence
from sqlalchemy import Column, String, DateTime, Integer, Float, Date, MetaData
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.sql import select
from sqlalchemy import update, delete

from json import loads, dumps
from datetime import datetime, date

# Constants
LOGPREFIX = "myHealth"

# declarative base class
Base = declarative_base()

def submit_feedback(event, context):
    print("{} - Submit Feedback".format(LOGPREFIX))
    
    response_payload = {}
    
    # Get input parameter
    user_id = None
    if event.get("pathParameters") and event.get("pathParameters").get("userid"):
        user_id = event.get("pathParameters").get("userid")
    else:
        response_payload = {
            "message": "Missing User ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    # Get input parameter
    payload = loads(event.get("body"))
    
    if (payload):
        
        mssql_url = get_parameter_value('serverless-mssql-url')
        row = mapping_FEEDBACK(payload)
        engine = create_engine(mssql_url)

        # create session and add objects
        with Session(engine) as session:
            feedback_id = session.execute(Sequence("FEEDBACK_SEQ"))
            print("{} - Feedback id: {}".format(LOGPREFIX, feedback_id))
            row.ID = feedback_id
            print("{} - DEBUG 1".format(LOGPREFIX))
            row.USER_ID = user_id
            print("{} - DEBUG 2".format(LOGPREFIX))
            session.add(row)
            print("{} - DEBUG 3".format(LOGPREFIX))
            session.commit()
            
            response_payload = {
                "FEEDBACK_ID": feedback_id
            }
            
    else:
        response_payload = {
            "message": "Missing Payload"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
        
    return {"statusCode": 200, "body": dumps(response_payload, default=str), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
def get_feedback(event, context):
    print("{} - Get Feedback".format(LOGPREFIX))
    
    response_payload = {}
    list_feedback = []
    
    # Get input parameter
    user_id = None
    if event.get("pathParameters") and event.get("pathParameters").get("userid"):
        user_id = event.get("pathParameters").get("userid")
    else:
        response_payload = {
            "message": "Missing User ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    with Session(engine) as session:
        qs = session.query(FEEDBACK).filter(FEEDBACK.USER_ID == user_id)
        
        for q in qs:
            feedback = {
                "ID": q.ID if q.ID else "",
                "USER_ID": q.USER_ID.strip() if q.USER_ID else "",
                "MESSAGE_T": q.MESSAGE_T.strip() if q.MESSAGE_T else "",
                "RATING": q.RATING if q.RATING else ""
            }
            list_feedback.append(feedback)
            
    return {"statusCode": 200, "body": dumps(list_feedback, default=str), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    
def get_parameter_value(key):
    print("{} - Get Parameter key: {}".format(LOGPREFIX, key))
    ssm_client = boto3.client("ssm")
    value = ssm_client.get_parameter(Name=key, WithDecryption=False)
    return value.get("Parameter").get("Value")


# Table FEEDBACK
class FEEDBACK(Base):
    __tablename__ = 'FEEDBACK'
    
    ID  = Column("ID", String, primary_key=True)
    USER_ID = Column("USER_ID", String)
    MESSAGE_T = Column("MESSAGE_T", String)
    RATING = Column("RATING", Integer)
    CREATED_DT = Column("CREATED_DT", DateTime)
    UPDATED_DT = Column("UPDATED_DT", DateTime)
    
def mapping_FEEDBACK(data):
    currentDateTime = datetime.now()
    return FEEDBACK(
        MESSAGE_T = data.get("MESSAGE_T"),
        RATING = data.get("RATING"),
        CREATED_DT = currentDateTime,
        UPDATED_DT = currentDateTime
    )
    