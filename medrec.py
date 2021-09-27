import pymssql
import boto3
import uuid

from sqlalchemy.orm import declarative_base
from sqlalchemy.schema import Sequence
from sqlalchemy import Column, String, DateTime, Integer, Float, Date, MetaData
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.sql import select
from sqlalchemy import update, delete

from json import loads, dumps
from datetime import datetime, date, timedelta
from random import randrange, randint, uniform

# Constants
LOGPREFIX = "myHealth"

# declarative base class
Base = declarative_base()

def submit_medical_record(event, context):
    print("{} - Submit Medical Record (Patient)".format(LOGPREFIX))
    
    response_payload = {}
    
    # Get Input Parameter
    payload = loads(event.get("body"))
    print("{} - Payload: {}".format(LOGPREFIX, payload))
    
    if payload:
        row = mapping_MEDICAL_RECORD(payload)
        mssql_url = get_parameter_value('serverless-mssql-url')
        engine = create_engine(mssql_url)
        
        medrec_id = None
        with Session(engine) as session:
            
            try: 
                medrec_id = session.execute(Sequence("MEDICAL_RECORD_SEQ"))
                row.ID = medrec_id
                session.add(row)
                
            
                if (payload.get("MEDICATION")):
                    submit_medication(session, payload.get("MEDICATION"), row.ID)  
                    
                response_payload = {
                    "ID": medrec_id
                }
                session.commit()
                
                # send payload to SQS
                queue_url = get_parameter_value('serverless-recommendation-queue-url')
                client = boto3.client('sqs')
                response = client.send_message(QueueUrl=queue_url, MessageBody=dumps(payload))
                print("{} - Payload has been pushed to SQS".format(LOGPREFIX))
                
            except Exception as e:
                print("{} - Exception: {}".format(LOGPREFIX, str(e)))
                session.rollback()
        
    else:
        response_payload = {
            "message": "Missing Payload"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
        
    print("{} - Response Payload: {}".format(LOGPREFIX, response_payload))
    return {"statusCode": 200, "body": dumps(response_payload, default=str), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}        
    
    
def update_medical_record(event, context):
    print("{} - Update Medical Record".format(LOGPREFIX))

    response_payload = {}
    
    # Get Input Parameter
    patient_id = None
    if event.get("pathParameters") and event.get("pathParameters").get("patientId"):
        patient_id = event.get("pathParameters").get("patientId")
    else:
        response_payload = {
            "message": "Missing Patient ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    print("{} - Patient ID: {}".format(LOGPREFIX, patient_id))
    
    payload = loads(event.get("body"))
    print("{} - Payload: {}".format(LOGPREFIX, payload))
    
    if payload:
        mssql_url = get_parameter_value('serverless-mssql-url')
        engine = create_engine(mssql_url)
        
        medrec_id = payload.get("MEDREC_ID")
    
        with Session(engine) as session:
            try:
                stmt = update(MEDICAL_RECORD).where(MEDICAL_RECORD.ID == medrec_id).values(payload).execution_options(synchronize_session="fetch")
                session.execute(stmt)
                
                if payload.get("MEDICATION"):
                    delete_medication(session, medrec_id)
                    submit_medication(session, payload.get("MEDICATION"), medrec_id)  
                    
                response_payload = {
                    "ID": medrec_id
                }
                session.commit()
            except Exception as e:
                print("{} - Exception: {}".format(LOGPREFIX, str(e)))
                session.rollback()
    else:
        response_payload = {
            "message": "Missing Payload"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
        
    return {"statusCode": 200, "body": dumps(response_payload, default=str), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}        
    
def delete_medical_record(event, context):
    print("{} - Delete Medical Record".format(LOGPREFIX))

    response_payload = {}
    
    # Get Input Parameter
    patient_id = None
    if event.get("pathParameters") and event.get("pathParameters").get("patientId"):
        patient_id = event.get("pathParameters").get("patientId")
    else:
        response_payload = {
            "message": "Missing Patient ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    print("{} - Patient ID: {}".format(LOGPREFIX, patient_id))
    
    medrec_id = None
    if event.get("pathParameters") and event.get("pathParameters").get("medrecId"):
        medrec_id = event.get("pathParameters").get("medrecId")
    else:
        response_payload = {
            "message": "Missing Medical Record ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    print("{} - Medical Record: {}".format(LOGPREFIX, medrec_id))
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    with Session(engine) as session:
        try:
            delete_medication(session, medrec_id)
            stmt = delete(MEDICAL_RECORD).where(MEDICAL_RECORD.ID == medrec_id).execution_options(synchronize_session="fetch")
            result = session.execute(stmt)
            session.commit()
        except Exception as e:
            print("{} - Exception: {}".format(LOGPREFIX, str(e)))
            session.rollback()
        
    print("{} - Delete is done".format(LOGPREFIX))
        
    return {"statusCode": 204, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
def get_medical_record(event, context):
    print("{} - Get Medical Record".format(LOGPREFIX))
    
    list_medical_record = []
    
    # Get Input Parameter
    patient_id = None
    if event.get("pathParameters") and event.get("pathParameters").get("patientId"):
        patient_id = event.get("pathParameters").get("patientId")
    else:
        response_payload = {
            "message": "Missing Patient ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    print("{} - Patient ID: {}".format(LOGPREFIX, patient_id))
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    with Session(engine) as session:
        query =  """
                 SELECT A.ID, A.PATIENT_ID, A.PROBLEM, A.DIAGNOSIS, A.ASSESSMENT, A.CREATED_BY AS CREATED_BY_ID, B.FULLNAME
                 FROM MEDICAL_RECORD A, APP_USER B
                 WHERE A.CREATED_BY = B.USER_ID
                 AND A.PATIENT_ID = :PATIENT_ID
                 ORDER BY A.CREATED_DT DESC;
                 """
                 
        qs = session.execute(query, {"PATIENT_ID": patient_id})
        
        for q in qs:
            medrec = {
                "MEDREC_ID": q.ID,
                "PATIENT_ID": q.PATIENT_ID.strip(),
                "PROBLEM": q.PROBLEM.strip() if q.PROBLEM else "",
                "DIAGNOSIS": q.DIAGNOSIS.strip() if q.DIAGNOSIS else "",
                "ASSESSMENT": q.ASSESSMENT.strip() if q.ASSESSMENT else "",
                "CREATED_BY_ID": q.CREATED_BY_ID.strip() if q.CREATED_BY_ID else "",
                "CREATED_BY_NAME": q.FULLNAME.strip() if q.FULLNAME else "",
                "MEDICATION": []
            }
            print("{} - Medical Record:{} ".format(LOGPREFIX, medrec))
            list_medical_record.append(medrec)
            
        for medrec in list_medical_record:
            medrec_id = medrec.get("MEDREC_ID")
            medrec['MEDICATION'] = get_medication(session, medrec_id)
            
            
    print("{} - List Medical Record: {}".format(LOGPREFIX, list_medical_record))
            
    return {"statusCode": 200, "body": dumps(list_medical_record, default=str), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}        
        
def get_medical_record_by_id(event, context):
    print("{} - Get Medical Record by ID".format(LOGPREFIX))
    
    list_medical_record = []
    
    # Get Input Parameter
    medrec_id = None
    if event.get("pathParameters") and event.get("pathParameters").get("medrecId"):
        medrec_id = event.get("pathParameters").get("medrecId")
    else:
        response_payload = {
            "message": "Missing Medical Record ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    print("{} - Medical Record ID: {}".format(LOGPREFIX, medrec_id))
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    with Session(engine) as session:
        query =  """
                 SELECT A.ID, A.PATIENT_ID, A.PROBLEM, A.DIAGNOSIS, A.ASSESSMENT, A.CREATED_BY AS CREATED_BY_ID, B.FULLNAME
                 FROM MEDICAL_RECORD A, APP_USER B
                 WHERE A.CREATED_BY = B.USER_ID
                 AND A.ID = :MEDREC_ID
                 ORDER BY A.CREATED_DT DESC;
                 """
                 
        qs = session.execute(query, {"MEDREC_ID": medrec_id})
        
        for q in qs:
            medrec = {
                "MEDREC_ID": q.ID,
                "PATIENT_ID": q.PATIENT_ID.strip(),
                "PROBLEM": q.PROBLEM.strip() if q.PROBLEM else "",
                "DIAGNOSIS": q.DIAGNOSIS.strip() if q.DIAGNOSIS else "",
                "ASSESSMENT": q.ASSESSMENT.strip() if q.ASSESSMENT else "",
                "CREATED_BY_ID": q.CREATED_BY_ID.strip() if q.CREATED_BY_ID else "",
                "CREATED_BY_NAME": q.FULLNAME.strip() if q.FULLNAME else "",
                "MEDICATION": []
            }
            print("{} - Medical Record:{} ".format(LOGPREFIX, medrec))
            list_medical_record.append(medrec)
            
        for medrec in list_medical_record:
            medrec_id = medrec.get("MEDREC_ID")
            medrec['MEDICATION'] = get_medication(session, medrec_id)
            
            
    print("{} - List Medical Record: {}".format(LOGPREFIX, list_medical_record))
            
    return {"statusCode": 200, "body": dumps(list_medical_record, default=str), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}        
        
def submit_medication(session, payload, medrec_id):
    print("{} - Submit Medication".format(LOGPREFIX))
    print("{} - Payload: {}".format(LOGPREFIX, payload))
    print("{} - Medical Record ID: {}".format(LOGPREFIX, medrec_id))
    
    for medication in payload:
        med_id = session.execute(Sequence("MEDICATION_SEQ"))
        medication["ID"] = med_id
        medication["MEDREC_ID"] = medrec_id
        row = mapping_MEDICATION(medication)
        session.add(row)
    
    print("{} - Submit Medication is done".format(LOGPREFIX))
    
def update_medication(session, payload):
    print("{} - Update Medication".format(LOGPREFIX))
    print("{} - Payload: {}".format(LOGPREFIX, payload))
    
    for medication in payload:
        stmt = update(MEDICATION).where(MEDICATION.ID == medication.get("ID")).values(payload).execution_options(synchronize_session="fetch")
        session.execute(stmt)
    
    print("{} - Update Medication is done")
    
def get_medication(session, medrec_id):
    print("{} - Get Medication".format(LOGPREFIX))
    print("{} - Medical Record ID: {}".format(LOGPREFIX, medrec_id))
    
    list_medication = []
    
    qs = session.query(MEDICATION).filter(MEDICATION.MEDREC_ID == medrec_id)
    
    for q in qs:
        medication = {
            "ID": q.ID,
            "MEDREC_ID": q.MEDREC_ID,
            "NAME_T": q.NAME_T.strip(),
            "QUANTITY": q.QUANTITY,
            "NOTES": q.NOTES.strip() if q.NOTES else ""
        }
        list_medication.append(medication)
    
    print("{} - List Medication: {}".format(LOGPREFIX, list_medication))
    return list_medication
    
def delete_medication(session, medrec_id):
    print("{} - Delete Medication".format(LOGPREFIX))
    print("{} - Medical Record ID: {}".format(LOGPREFIX, medrec_id))
    
    stmt = delete(MEDICATION).where(MEDICATION.MEDREC_ID == medrec_id).execution_options(synchronize_session="fetch")
    result = session.execute(stmt)
    
    print("{} - Delete Medication is done".format(LOGPREFIX))
        
    
def get_parameter_value(key):
    print("{} - Get Parameter key: {}".format(LOGPREFIX, key))
    ssm_client = boto3.client("ssm")
    value = ssm_client.get_parameter(Name=key, WithDecryption=False)
    return value.get("Parameter").get("Value")
    
class MEDICAL_RECORD(Base):
    __tablename__ = "MEDICAL_RECORD"
    
    ID = Column("ID", Integer, primary_key=True, autoincrement=False)
    PATIENT_ID = Column("PATIENT_ID", String)
    PROBLEM = Column("PROBLEM", String)
    DIAGNOSIS = Column("DIAGNOSIS", String)
    ASSESSMENT = Column("ASSESSMENT", String)
    CREATED_DT = Column("CREATED_DT", DateTime)
    UPDATED_DT = Column("UPDATED_DT", DateTime)
    CREATED_BY = Column("CREATED_BY", String)
    
def mapping_MEDICAL_RECORD(data):
    currentDateTime = datetime.now()
    
    return MEDICAL_RECORD(
        ID=data.get("ID"),
        PATIENT_ID=data.get("PATIENT_ID"),
        PROBLEM=data.get("PROBLEM"),
        DIAGNOSIS=data.get("DIAGNOSIS"),
        ASSESSMENT=data.get("ASSESSMENT"),
        CREATED_BY=data.get("CREATED_BY"),
        CREATED_DT=currentDateTime,
        UPDATED_DT=currentDateTime
        )
    
class MEDICATION(Base):
    __tablename__ = "MEDICATION"
    
    ID = Column("ID", Integer, primary_key=True, autoincrement=False)
    MEDREC_ID = Column("MEDREC_ID", Integer)
    NAME_T = Column("NAME_T", String)
    QUANTITY = Column("QUANTITY", Float)
    NOTES = Column("NOTES", String)
    CREATED_DT = Column("CREATED_DT", DateTime)
    UPDATED_DT = Column("UPDATED_DT", DateTime)
    
def mapping_MEDICATION(data):
    currentDateTime = datetime.now()
    
    return MEDICATION(
        ID=data.get("ID"),
        MEDREC_ID=data.get("MEDREC_ID"),
        NAME_T=data.get("NAME_T"),
        QUANTITY=data.get("QUANTITY"),
        NOTES=data.get("NOTES"),
        CREATED_DT=currentDateTime,
        UPDATED_DT=currentDateTime
        )