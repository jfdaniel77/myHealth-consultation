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
CONSULTATION_REMINDER_DAYS = 2

# declarative base class
Base = declarative_base()

def consultation_reminder(event, context):
    print("{} - Consultation Reminder".format(LOGPREFIX))
    
    # Get list of consulation
    list_consultation = get_consulation_slot()
    
    ses_sender_arn = get_parameter_value("serverless-ses-sender-identity")
    
    client = boto3.client('ses')
    
    for consultation in list_consultation:
        
        location = ""
        
        if consultation.get("LOCATION") == "ONLINE":
            location = consultation.get("LOCATION_URL")
        else:
            location = "{} - {}".format(consultation.get("LOCATION_NAME"), consultation.get("LOCATION_ADDRESS"))
        
        message = get_message(consultation.get("PATIENT_NAME"), 
                            consultation.get("DOCTOR_NAME"),
                            location, 
                            consultation.get("APPT_DT"),
                            consultation.get("APPT_SLOT"))
        
        response = client.send_email(
            Source='myhealthapp.iss@gmail.com',
            Destination={
                'ToAddresses': [
                    'jfdaniel77@gmail.com',
                ]
            },
            Message={
                'Subject': {
                    'Data': 'Reminder - Upcoming Consultation'
                },
                'Body': {
                    'Html': {
                        'Data': message
                    }
                }
            },
            SourceArn= ses_sender_arn
        )
        
        print("{} - Response: {}".format(LOGPREFIX, response))
        
        return {"statusCode": 200}
            
def get_consulation_slot():
    print("{} - Get Consulation Slot".format(LOGPREFIX))
    
    list_consulation = []
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    with Session(engine) as session:
        query = """
                SELECT B.FULLNAME AS PATIENT_NAME, C.FULLNAME AS DOCTOR_NAME, D.LOCATION_NAME, D.LOCATION_ADDRESS, A.LOCATION_URL, A.APPT_DT, A.APPT_SLOT,B.EMAIL 
                FROM APPOINTMENT A, APP_USER B, APP_USER C, LOCATION D
                WHERE A.APPT_DT = DATEADD(dd, :CONSULTATION_REMINDER_DAYS, CAST(GETDATE() AS DATE))
                AND B.USER_ID = A.PATIENT_ID
                AND C.USER_ID = A.DOCTOR_ID
                AND D.LOCATION_C = A.LOCATION_C;
                """
                
        qs = session.execute(query, {"CONSULTATION_REMINDER_DAYS": 2})
        
        for q in qs:
            consultation = {
                "PATIENT_NAME": q.PATIENT_NAME.strip(),
                "DOCTOR_NAME": q.DOCTOR_NAME.strip(),
                "LOCATION_NAME": q.LOCATION_NAME.strip(),
                "LOCATION_ADDRESS": q.LOCATION_ADDRESS.strip(),
                "LOCATION_URL": q.LOCATION_URL.strip(),
                "APPT_DT": q.APPT_DT.strftime('%d/%m/%Y'),
                "APPT_SLOT": q.APPT_SLOT.strip(),
                "EMAIL": q.EMAIL.strip()
            }
            list_consulation.append(consultation)
            
    return list_consulation
        
def get_parameter_value(key):
    print("{} - Get Parameter key: {}".format(LOGPREFIX, key))
    ssm_client = boto3.client("ssm")
    value = ssm_client.get_parameter(Name=key, WithDecryption=False)
    return value.get("Parameter").get("Value")
    
def get_message(patientName, doctorName, location, date, slot):
    return """
            <html>
                <body>
                <p>Dear {},</p>
                <br>
                <p>You have an upcoming appointment with {} at {} on {} at {}.</p>
                <br>
                <p>Thank you<br>myHealth</p>
                <br>
                <p>This is a system generated email.</p>
                </body>
            </html>
            """.format(patientName, doctorName, location, date, slot)