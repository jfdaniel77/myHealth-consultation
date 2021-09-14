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
TIMEDELTA = 30 # 1 MONTH
START_HOUR = 8
END_HOUR = 17
START_LUNCH_HOUR = 12
END_LUNCH_HOUR = 14
GRACE_PERIOD = 2
TIME_SLOT = ["00","15", "30", "45"]

# declarative base class
Base = declarative_base()

def health_check(event, context):
    body = {
        "message": "Welcome to myHealth Service. It is built using Serverless v2.0! Your function executed successfully!"
    }

    response = {"statusCode": 200, "body": dumps(body)}

    return response
    
def create_appointment_booking(event, contect):
    print("{} - Create Appointment Booking".format(LOGPREFIX))
    
    response_payload = {}
    
    # Get Input Parameter
    payload = loads(event.get("body"))
    print("{} - Payload: {}".format(LOGPREFIX, payload))
    
    if payload:
        # Generate UUID Token
        token = str(uuid.uuid4())
        payload['TOKEN'] = token
        print("{} - Token: {}".format(LOGPREFIX, token))
        
        # Store into MongoDB
        client = boto3.client("dynamodb")
        response = client.put_item(
                        TableName="myhealth-appointment",
                        Item={
                            "token": {
                                "S": token
                            },
                            "booking_status": {
                                "S": "IN PROGRESS"
                            },
                            "input": {
                                "S": dumps(payload)
                            }
                        })
                        
        print("{} - Payload has been stored in DynamoDB".format(LOGPREFIX))
        
        # Push input parameter to SQS
        queue_url = get_parameter_value('serverless-appointment-queue-url')
        client = boto3.client('sqs')
        response = client.send_message(QueueUrl=queue_url, MessageBody=dumps(payload), MessageGroupId='appointmentBooking')
        print("{} - Token has been pushed to SQS".format(LOGPREFIX))
        
        # Return UUID Token
        response_payload = {
            "TOKEN": token
        }
    else:
        response_payload = {
            "message": "Missing Payload"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
        
    return {"statusCode": 200, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}        

def get_appointment_booking_status(event, context):
    print("{} - Get Appointment Booking Status".format(LOGPREFIX))
    
    response_payload = {}
    
    # Get input parameter
    token = None
    if event.get("pathParameters") and event.get("pathParameters").get("token"):
        token = event.get("pathParameters").get("token")
    else:
        response_payload = {
            "message": "Missing Token"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    print("{} - Token: {}".format(LOGPREFIX, token))
    client = boto3.client('dynamodb')    
    response = client.get_item(
                        TableName="myhealth-appointment",
                        Key={
                            "token": {
                                'S': token
                                }
                            })
    
    if response.get("Item"):
        
        if response.get("Item").get("booking_status").get("S") == "IN PROGRESS":
            response_payload = {
                "TOKEN": token,
                "BOOKING_STATUS": response.get("Item").get("booking_status").get("S")
            }
        else:
            response_payload = {
                "TOKEN": token,
                "BOOKING_STATUS": response.get("Item").get("booking_status").get("S")
            }
        print("{} - Response Payload: {}".format(LOGPREFIX, dumps(response_payload)))
    else:
        response_payload = {
            "message": "Incorrect Token"   
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
                            
    return {"statusCode": 200, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}        
        
def confirm_appointment_booking(event, context):
    print("{} - Confirm Appointment Booking".format(LOGPREFIX))
    
    response_payload = {}
    
    # Get input parameter
    token = None
    if event.get("pathParameters") and event.get("pathParameters").get("token"):
        token = event.get("pathParameters").get("token")
    else:
        response_payload = {
            "message": "Missing Token"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    print("{} - Token: {}".format(LOGPREFIX, token))
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    # create session and add objects
    with Session(engine) as session:
        query = """
                UPDATE APPOINTMENT SET STATUS = 'CONFIRMED' WHERE TOKEN = :TOKEN
                """

        result = session.execute(query, {"TOKEN": token})
        session.commit()
    
        if result.rowcount > 0:
            response_payload = {
                "TOKEN": token,
                "BOOKING_STATUS": "CONFIRMED"
            }
            print("{} - Payload: {}".format(LOGPREFIX, response_payload))
        else:
            response_payload = {
                "message": "Invalid token"
            }
            print("{} - Payload: {}".format(LOGPREFIX, response_payload))
            return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}

    return {"statusCode": 200, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
def purge_appointment_booking(event, context):
    print("{} - Purge Appointment Booking".format(LOGPREFIX))
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    tempTime = datetime.now() - timedelta(minutes=15)
    print("{} - TempTime: {}".format(LOGPREFIX, tempTime))
    
    # create session and add objects
    with Session(engine) as session:
        stmt = delete(APPOINTMENT).where(APPOINTMENT.STATUS == 'BOOKED').where(APPOINTMENT.CREATED_DT < tempTime).execution_options(synchronize_session="fetch")
        result = session.execute(stmt)
        session.commit()
        
    print("{} - Purge Appointment Booking done".format(LOGPREFIX))

def store_appointment_booking(event, context):
    print("{} - Store Appointment Booking".format(LOGPREFIX))
    
    # Get Payload
    print("{} - Number of records = {}".format(LOGPREFIX, len(event.get("Records"))))
    if len(event.get("Records")) > 0:
        for record in event.get("Records"):
            
            input_parameter = loads(record.get("body"))
            print("{} - Payload from SQS: {}".format(LOGPREFIX, input_parameter))
            
            # Set required values to local variables
            patient_id = input_parameter.get("PATIENT_ID")
            doctor_id = input_parameter.get("DOCTOR_ID")
            location_c = input_parameter.get("LOCATION_C")
            appt_date = input_parameter.get("APPT_DT")
            appt_slot = input_parameter.get("APPT_SLOT")
            token = input_parameter.get("TOKEN")
            
            location_url = ""
            if is_location_online(location_c):
                location_url = generate_fake_zoom_link()
                
            payload = {
                "ID": "",
            	"PATIENT_ID": patient_id,
            	"DOCTOR_ID": doctor_id,
            	"LOCATION_C": location_c,
            	"LOCATION_URL": location_url,
            	"APPT_DT": appt_date,
            	"APPT_SLOT": appt_slot,
            	"TOKEN": token,
            	"STATUS": ""
            }
            
            # Store data to MSSQL
            mssql_url = get_parameter_value('serverless-mssql-url')
            engine = create_engine(mssql_url)
            
            # create session and add objects
            with Session(engine) as session:
                query = """
                        SELECT COUNT(*) AS RESULT
                        FROM APPOINTMENT
                        WHERE DOCTOR_ID = :DOCTOR_ID
                        AND APPT_DT = CONVERT(DATETIME, :APPT_DT, 103)
                        AND APPT_SLOT = :APPT_SLOT
                        AND (STATUS = 'CONFIRMED' OR STATUS = 'BOOKED')
                        """
                        
                qs = session.execute(query, {
                    "DOCTOR_ID": doctor_id,
                    "APPT_DT": appt_date,
                    "APPT_SLOT": appt_slot
                })
                
                result = 0
                
                for q in qs:
                    result = int(q.RESULT)
                    
                if result == 0:
                    # if No Record
                    booked_appointment(payload)
                    update_record_dynamodb(token, "BOOKED")
                else:
                    # if There is record
                    update_record_dynamodb(token, "REJECTED")
    
    return ""
    
def booked_appointment(payload):
    print("{} - Create Appointment record process".format(LOGPREFIX))
    
    # Store data to MSSQL
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    # create session and add objects
    with Session(engine) as session:
        # Get sequence number
        payload["ID"] = session.execute(Sequence("APPOINTMENT_SEQ"))
        payload["STATUS"] = "BOOKED"
        print("{} - Payload: {}".format(LOGPREFIX, payload))
        row = mapping_APPOINTMENT(payload)
        session.add(row)
        session.commit()
        
    print("{} - Appointment has been booked".format(LOGPREFIX))
    
def complete_appointment(event, context):
    print("{} - Set Complete for old appointment".format(LOGPREFIX))
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    with Session(engine) as session:
        query = """
                UPDATE APPOINTMENT SET STATUS = 'DONE' WHERE APPT_DT < GETDATE()
                """
        result = session.execute(query)
        print("{} - Update: {} record".format(LOGPREFIX, result.rowcount))
        session.commit()
    print("{} - Set Complete for old appointment done".format(LOGPREFIX))
    
          
def get_patient_appointment(event, context):
    print("{} - Get Patient Appointment".format(LOGPREFIX))
    
    list_appoinment = []
    
    # Get input parameter
    user_id = None
    if event.get("pathParameters") and event.get("pathParameters").get("patientId"):
        user_id = event.get("pathParameters").get("patientId")
    else:
        response_payload = {
            "message": "Missing Patient ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    print("{} - Patient ID: {}".format(LOGPREFIX, user_id))
    
    list_appointment = get_patient_slot(user_id)
            
    return {"statusCode": 200, "body": dumps(list_appointment, default=str), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}        
            
def delete_patient_appointment(event, context):
    print("{} - Delete Patient Appointment".format(LOGPREFIX))
    
    # Get input parameter
    appointment_id = None
    if event.get("pathParameters") and event.get("pathParameters").get("appointmentId"):
        appointment_id = event.get("pathParameters").get("appointmentId")
    else:
        response_payload = {
            "message": "Missing Appointment ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    print("{} - Appointment ID: {}".format(LOGPREFIX, appointment_id))
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    with Session(engine) as session:
        stmt = delete(APPOINTMENT).where(APPOINTMENT.ID == appointment_id).execution_options(synchronize_session="fetch")
        session.execute(stmt)
        session.commit()
        
    print("{} - Delete is done".format(LOGPREFIX))
        
    return {"statusCode": 204, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
def get_doctor_appointment(event, context):
    print("{} - Delete Patient Appointment".format(LOGPREFIX))
    
    # Get input parameter
    doctor_id = None
    if event.get("pathParameters") and event.get("pathParameters").get("doctorId"):
        doctor_id = event.get("pathParameters").get("doctorId")
    else:
        response_payload = {
            "message": "Missing Doctor ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    print("{} - Doctor ID: {}".format(LOGPREFIX, doctor_id))
    
    list_appointment = get_doctor_slot(doctor_id)
            
    return {"statusCode": 200, "body": dumps(list_appointment, default=str), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}  
    
def get_available_appointment_slot(event, context):
    print("{} - Get Appointment Slot".format(LOGPREFIX))
    
    # Get input parameter
    doctor_id = None
    if event.get("pathParameters") and event.get("pathParameters").get("doctorId"):
        doctor_id = event.get("pathParameters").get("doctorId")
    else:
        response_payload = {
            "message": "Missing Doctor ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    print("{} - Doctor ID: {}".format(LOGPREFIX, doctor_id))
    
    patient_id = None
    if event.get("pathParameters") and event.get("pathParameters").get("patientId"):
        patient_id = event.get("pathParameters").get("patientId")
    else:
        response_payload = {
            "message": "Missing Patient ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}
    
    print("{} - Patient ID: {}".format(LOGPREFIX, patient_id))
    
    doctor_slot = get_doctor_slot(doctor_id)
    patient_slot = get_patient_slot(patient_id)
    
    list_doctor_slot = []
    if len(doctor_slot) > 0:
        for slot in doctor_slot:
            temp_slot = "{} {}".format(slot["APPT_DT"], slot["APPT_SLOT"])
            list_doctor_slot.append(temp_slot)
            
    list_patient_slot = []
    if len(patient_slot) > 0:
        for slot in patient_slot:
            temp_slot = "{} {}".format(slot["APPT_DT"], slot["APPT_SLOT"])
            list_patient_slot.append(temp_slot)
            
    
    temp_appointment = generate_time_slot(list_doctor_slot, list_patient_slot)
    
    list_appointment = {}
    
    for appt in temp_appointment:
        val = appt.split(" ")
        appt_dt = val[0]
        appt_slot = val[1]
        
        if appt_dt in list_appointment.keys():
            list_appointment[appt_dt].append(appt_slot)
        else:
            appt_slot = [appt_slot]
            list_appointment[appt_dt] = appt_slot
            
    print("{} - Final available appointment: {}".format(LOGPREFIX, list_appointment))
    
    return {"statusCode": 200, "body": dumps(list_appointment, default=str), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}  
    
# Common Functions
def get_patient_slot(user_id):
    print("{} - Get Patient Slot".format(LOGPREFIX))
    
    list_appointment = []
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    with Session(engine) as session:
        query = """
                SELECT A.ID, A.DOCTOR_ID, D.FULLNAME, C.LOCATION_NAME, C.LOCATION_ADDRESS, A.LOCATION_URL, A.APPT_DT, A.APPT_SLOT, A.STATUS
                FROM APPOINTMENT A, APP_USER B, LOCATION C, APP_USER D
                WHERE A.PATIENT_ID = B.USER_ID
				AND A.DOCTOR_ID = D.USER_ID
				AND D.ROLE_C = '2'
                AND B.ROLE_C = '1'
                AND A.LOCATION_C = C.LOCATION_C
                AND A.PATIENT_ID = :PATIENT_ID;
                """
        qs = session.execute(query, {
            "PATIENT_ID": user_id
        })
        
        for q in qs:
            data = {
                "ID": q.ID,
                "DOCTOR_ID": q.DOCTOR_ID.strip(),
                "DOCTOR_FULLNAME": q.FULLNAME.strip(),
                "LOCATION_NAME": q.LOCATION_NAME.strip(),
                "LOCATION_ADDRESS": q.LOCATION_ADDRESS.strip(),
                "LOCATION_URL": q.LOCATION_URL.strip(),
                "APPT_DT": q.APPT_DT.strftime('%d/%m/%Y'),
                "APPT_SLOT": q.APPT_SLOT.strip(),
                "STATUS": q.STATUS.strip()
            }
            list_appointment.append(data)
            
    print("{} - List Appointment: {}".format(LOGPREFIX, list_appointment))
    
    return list_appointment
    
def get_doctor_slot(doctor_id):
    print("{} - Get Doctor Slot".format(LOGPREFIX))
    
    list_appointment = []
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    with Session(engine) as session:
        query = """
                SELECT A.ID, A.PATIENT_ID, D.FULLNAME, C.LOCATION_NAME, C.LOCATION_ADDRESS, A.LOCATION_URL, A.APPT_DT, A.APPT_SLOT, A.STATUS
                FROM APPOINTMENT A, APP_USER B, LOCATION C, APP_USER D
                WHERE A.DOCTOR_ID = B.USER_ID
				AND A.PATIENT_ID = D.USER_ID
				AND D.ROLE_C = '1'
                AND B.ROLE_C = '2'
                AND A.LOCATION_C = C.LOCATION_C
                AND A.DOCTOR_ID = :DOCTOR_ID;
                """
        qs = session.execute(query, {
            "DOCTOR_ID": doctor_id
        })
        
        for q in qs:
            data = {
                "ID": q.ID,
                "PATIENT_ID": q.PATIENT_ID.strip(),
                "PATIENT_FULLNAME": q.FULLNAME.strip(),
                "LOCATION_NAME": q.LOCATION_NAME.strip(),
                "LOCATION_ADDRESS": q.LOCATION_ADDRESS.strip(),
                "LOCATION_URL": q.LOCATION_URL.strip(),
                "APPT_DT": q.APPT_DT.strftime('%d/%m/%Y'),
                "APPT_SLOT": q.APPT_SLOT.strip(),
                "STATUS": q.STATUS.strip()
            }
            list_appointment.append(data)
            
    print("{} - List Appointment: {}".format(LOGPREFIX, list_appointment))
    
    return list_appointment
    
def update_record_dynamodb(token, status):
    print("{} - Update Record in DynamoDB".format(LOGPREFIX))
    
    client = boto3.client("dynamodb")
    response = client.update_item(
                                TableName="myhealth-appointment",
                                Key={
                                    "token": {
                                        "S": token
                                    }
                                },
                                UpdateExpression="set booking_status = :val1",
                                ExpressionAttributeValues={
                                    ":val1": {
                                        "S": status
                                        
                                    }
                                },
                                ReturnValues="UPDATED_NEW"
                            )
    print("{} - New Record: {}".format(LOGPREFIX, response))
            
def get_parameter_value(key):
    print("{} - Get Parameter key: {}".format(LOGPREFIX, key))
    ssm_client = boto3.client("ssm")
    value = ssm_client.get_parameter(Name=key, WithDecryption=False)
    return value.get("Parameter").get("Value")
    
def generate_fake_zoom_link():
    print("{} - Generate fake zoom".format(LOGPREFIX))
    digits = randint(11111111111,99999999999)
    temp = str(uuid.uuid4()).split("-")
    value = "".join(temp)
    zoom_link = "https://myhealth.zoom.us/j/{}?pwd={}".format(digits, value)
    print("{} - Zoom link: {}".format(LOGPREFIX, zoom_link))
    return zoom_link
    
def is_location_online(code):
    print("{} - Is Location online?".format(LOGPREFIX))
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    # create session and add objects
    with Session(engine) as session:
        qs = session.query(LOCATION).filter(LOCATION.LOCATION_C == code)
        for q in qs:
            temp = q.LOCATION_NAME.strip()
            
            if temp == "ONLINE":
                print("{} - {} is ONLINE".format(LOGPREFIX, code))
                return True
            else:
                print("{} - {} is not ONLINE".format(LOGPREFIX, code))
                return False
    
    
def generate_time_slot(doctor_slot, patient_slot):
    print("{} - Generate Time Slot".format(LOGPREFIX))
    today = datetime.now()
    
    list_available_slot = []
    
    for delta in range(0, TIMEDELTA):
        date = today + timedelta(delta)    
        
        if date.weekday() >= 5:
            continue
    
        for idx in range(START_HOUR, END_HOUR):
            
            if idx >= START_LUNCH_HOUR and idx < END_LUNCH_HOUR:
                continue
            
            if delta == 0 and idx <= int(date.strftime('%H')) + GRACE_PERIOD:
                continue
                
            val_time = str(idx)
            if len(val_time) == 1:
                val_time = "0{}".format(val_time)
                
            for slot in TIME_SLOT:
                slot = "{} {}.{}".format(date.strftime('%d/%m/%Y'), val_time, slot)
                
                if slot in doctor_slot or slot in patient_slot:
                    continue
                    
                list_available_slot.append(slot)    

    print("{} - List available initial slot: {}".format(LOGPREFIX, list_available_slot))
    return list_available_slot
    
# Table - APPOINTMENT
class APPOINTMENT(Base):
	__tablename__ = "APPOINTMENT"

	ID = Column("ID", Integer, primary_key=True, autoincrement=False)
	PATIENT_ID = Column("PATIENT_ID", String)
	DOCTOR_ID = Column("DOCTOR_ID", String)
	LOCATION_C = Column("LOCATION_C", Integer)
	LOCATION_URL = Column("LOCATION_URL", String)
	CREATED_DT = Column("CREATED_DT", DateTime)
	UPDATED_DT = Column("UPDATED_DT", DateTime)
	APPT_DT = Column("APPT_DT", Date)
	APPT_SLOT = Column("APPT_SLOT", String)
	STATUS = Column("STATUS", String)
	TOKEN = Column("TOKEN", String)
	
def mapping_APPOINTMENT(data):
    currentDateTime = datetime.now()
    
    return APPOINTMENT(
        ID = data.get("ID"),
    	PATIENT_ID = data.get("PATIENT_ID"),
    	DOCTOR_ID = data.get("DOCTOR_ID"),
    	LOCATION_C = data.get("LOCATION_C"),
    	LOCATION_URL = data.get("LOCATION_URL"),
    	CREATED_DT = currentDateTime,
    	UPDATED_DT = currentDateTime,
    	APPT_DT = datetime.strptime(data.get("APPT_DT"), "%d/%m/%Y"),
    	APPT_SLOT = data.get("APPT_SLOT"),
    	STATUS = data.get("STATUS"),
    	TOKEN = data.get("TOKEN")
        )
        

class LOCATION(Base):
	__tablename__ = "LOCATION"

	LOCATION_C = Column("LOCATION_C", Integer, primary_key=True)
	LOCATION_NAME = Column("LOCATION_NAME", String)
	LOCATION_ADDRESS = Column("LOCATION_ADDRESS", String)