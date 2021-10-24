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
from neo4j import GraphDatabase
from json import loads, dumps
from datetime import datetime, date, timedelta
from random import randrange, randint, uniform
from neo4j import GraphDatabase

# Constants
LOGPREFIX = "myHealth"
NUMBER_RECOMMENDATION = 10

# declarative base class
Base = declarative_base()

def get_recommendation(event, context):
    print("{} - Get Recommendation".format(LOGPREFIX))
    
    list_recommendation = []
    
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
    
    list_recommendation = get_recommendation_record(patient_id)
    list_recommendation = list_recommendation[:NUMBER_RECOMMENDATION]
    
    return {"statusCode": 200, "body": dumps(list_recommendation, default=str), "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}}        
    
def analyze_medical_problem(event, context):
    print("{} - Analyze Medical Problem".format(LOGPREFIX))
    
    # Get value from SQS
    print("{} - Number of records = {}".format(LOGPREFIX, len(event.get("Records"))))
    
    list_payload = []
    if len(event.get("Records")) > 0:
        for record in event.get("Records"):
            
            input_parameter = loads(record.get("body"))
            print("{} - Payload from SQS: {}".format(LOGPREFIX, input_parameter))
            
            problem = input_parameter.get("PROBLEM")
            diagnosis = input_parameter.get("DIAGNOSIS")
            assessment = input_parameter.get("ASSESSMENT")
            medication = input_parameter.get("MEDICATION")
            patient_id = input_parameter.get("PATIENT_ID")
            
            print("{} - Patient ID: {}".format(LOGPREFIX, patient_id))    
            print("{} - Problem: {}".format(LOGPREFIX, problem))    
            print("{} - Diagnosis: {}".format(LOGPREFIX, diagnosis))
            print("{} - Assessment: {}".format(LOGPREFIX, assessment))
            print("{} - Medication: {}".format(LOGPREFIX, medication))
            
            # Analyze clinical text
            # client = boto3.client(service_name='comprehendmedical', region_name='us-east-1')
            
            # list_entities = []
            # list_medical_condition = []
            # if problem:
            #     result_medical = client.detect_entities(Text=problem)
            #     entities = result_medical['Entities']
            #     list_entities.extend(entities)
            
            # if diagnosis:
            #     result_medical = client.detect_entities(Text=diagnosis)
            #     entities = result_medical['Entities']
            #     list_entities.extend(entities)
                
            # list_category = ["MEDICAL_CONDITION", "DIAGNOSIS"]
            # for entity in list_entities:
            #     if entity.get("Category") in list_category:
            #         if entity.get("Text") not in list_medical_condition:
            #             list_medical_condition.append(entity.get("Text"))
                        
            # print("{} - Condition: {}".format(LOGPREFIX, list_medical_condition))
            
            # # Get recommendation from GraphDB
            # list_recommendation = get_recommendation_graphdb(list_medical_condition)
            list_recommendation = []
            
            if list_recommendation and len(list_recommendation) > 0:
                message = " ; ".join(list_recommendation)
                print("{} - Recommendation to be stored to DB: {}".format(LOGPREFIX, message))
            
                # Store to MSSQL
                payload_db = {
                    "ID": "",
                    "PATIENT_ID": patient_id,
                    "MESSAGE_T": message,
                    "MSG_TP_T": "MEDICAL_RECORD"
                }
                list_payload.append(payload_db)
            
    insert_records(list_payload)
    
def analyze_vital_sign(event, context):
    print("{} - Analyze Vital Sign".format(LOGPREFIX))
    
    # Get value from SQS
    print("{} - Number of records = {}".format(LOGPREFIX, len(event.get("Records"))))
    
    list_payload = []
    if len(event.get("Records")) > 0:
        for record in event.get("Records"):
            
            input_parameter = loads(record.get("body"))
            print("{} - Payload from SQS: {}".format(LOGPREFIX, input_parameter))

            patient_id = input_parameter.get("USER_ID")            
            bp_value = input_parameter.get("BP")
            bt_value = input_parameter.get("BT")
            hr_value = input_parameter.get("HR")
            rr_value = input_parameter.get("RR")
            os_value = input_parameter.get("OS")
            bw_value = input_parameter.get("BW")
            bh_value = input_parameter.get("BH")
    
            # Analyze value
            # Body Temperature
            bt_resp = analyze_body_temperature(bt_value)
            if bt_resp["message"] != "NORMAL":
                payload_db = {
                    "ID": "",
                    "PATIENT_ID": patient_id,
                    "MESSAGE_T": bt_resp["message"],
                    "MSG_TP_T": "VITAL_SIGN"
                }
                list_payload.append(payload_db)
            
            # BMI
            bmi_resp = analyze_bmi(bh_value, bw_value)
            if bmi_resp["message"] != "NORMAL":
                payload_db = {
                    "ID": "",
                    "PATIENT_ID": patient_id,
                    "MESSAGE_T": bmi_resp["message"],
                    "MSG_TP_T": "VITAL_SIGN"
                }
                list_payload.append(payload_db)
    
            # Blood presure
            bp_resp = analyze_blood_pressure(bp_value)
            if bp_resp["message"] != "NORMAL":
                payload_db = {
                    "ID": "",
                    "PATIENT_ID": patient_id,
                    "MESSAGE_T": bp_resp["message"],
                    "MSG_TP_T": "VITAL_SIGN"
                }
                list_payload.append(payload_db)
            
            # Heart Rate
            hr_resp = analyze_heart_rate(hr_value, patient_id)
            if hr_resp["message"] != "NORMAL":
                payload_db = {
                    "ID": "",
                    "PATIENT_ID": patient_id,
                    "MESSAGE_T": hr_resp["message"],
                    "MSG_TP_T": "VITAL_SIGN"
                }
                list_payload.append(payload_db)
            
            # Oxigen Saturation Level 
            os_resp = analyze_o2_level(os_value)
            if os_resp["message"] != "NORMAL":
                payload_db = {
                    "ID": "",
                    "PATIENT_ID": patient_id,
                    "MESSAGE_T": os_resp["message"],
                    "MSG_TP_T": "VITAL_SIGN"
                }
                list_payload.append(payload_db)
            
            # Respiratory Rate
            rr_resp = analyze_respiratory_rate(rr_value)
            if rr_resp["message"] != "NORMAL":
                payload_db = {
                    "ID": "",
                    "PATIENT_ID": patient_id,
                    "MESSAGE_T": rr_resp["message"],
                    "MSG_TP_T": "VITAL_SIGN"
                }
                list_payload.append(payload_db)
            
    # Store to MSSQL
    insert_records(list_payload)
    
# Common functions
def analyze_body_temperature(value):
    print("{} - Analyze body temperature".format(LOGPREFIX))
    print("{} - Payload: {}".format(LOGPREFIX, value))
    
    response = {}
    
    if value > 37.5:
        response = {
            "message": "You have fever"
        }
    else:
        response = {
            "message": "NORMAL"
        }
    
    print("{} - Fever: {}".format(LOGPREFIX, response))    
    return response
    
def analyze_bmi(height, weight):
    print("{} - Analyze BMI".format(LOGPREFIX))
    print("{} - Payload: height: {} and weight: {}".format(LOGPREFIX, height, weight))
    
    bmi_response = None
    
    bmi = weight/((height/100)*(height/100))
    
    if bmi < 18.5 and bmi > 25:

        if bmi <= 18.5:
            bmi_response = {
                "bmi": bmi,
                "message": 'Your BMI is {} which means you are underweight.'.format(bmi)
            }
            
        elif bmi > 25 and bmi < 30:
            bmi_response = {
                "bmi": bmi,
                "message": 'Your BMI is {} which means you are overwight.'.format(bmi)
            }

        elif bmi > 30:
            bmi_response = {
                "bmi": bmi,
                "message": 'Your BMI is {} which means you are obese.'.format(bmi)
            }
    else: 
        bmi_response = {
            "bmi": bmi,
            "message": 'NORMAL'
        }

    print("{} - BMI: {}".format(LOGPREFIX, bmi_response))
    return bmi_response
    
def analyze_blood_pressure(value):
    print("{} - Analyze Blood Pressure".format(LOGPREFIX))
    print("{} - Payload: {}".format(LOGPREFIX, value))
    
    response = None
    temp = value.split("/")
    sys = int(temp[0])
    dias = int(temp[1])
    
    if sys <= 120 and dias <= 80:
        response = {
            "message": "NORMAL"
        }
    elif (sys > 120 and sys <= 140) and (dias > 80 and dias <= 89):
        response = {
            "message": "Your blood pressure is elevated (at borderline)"
        }
    elif sys > 140 and dias > 90:
        response = {
            "message": "Your blood pressure is high (hypertension)"
        }
    else:
        response = {
            "message": "NORMAL"
        }
    
    print("{} - Blood Pressure: {}".format(LOGPREFIX, response))
    return response
    
def analyze_heart_rate(value, patientId):
    print("{} - Analyze Heart Rate".format(LOGPREFIX))
    print("{} - Payload: patientId: {} and value: {}".format(LOGPREFIX, patientId, value))
    
    response = None

    # Get Age
    age = get_age(patientId)
    
    # Ref: https://my.clevelandclinic.org/health/diagnostics/17402-pulse--heart-rate
    if age < 25:
        if value < 120 and value > 170:
            response = {
                "message": "Your heart Rate is not normal"
            }
        else:
            response = {
                "message": "NORMAL"
            }
            
    elif age >=25 and age < 30:
        if value < 117 and value > 166:
            response = {
                "message": "Your heart Rate is not normal"
            }
        else:
            response = {
                "message": "NORMAL"
            }
            
    elif age >= 30 and age < 35:
        if value < 111 and value > 157:
            response = {
                "message": "Your heart Rate is not normal"
            }
        else:
            response = {
                "message": "NORMAL"
            }
            
    elif age >= 35 and age < 40:
        if value < 108 and value > 153:
            response = {
                "message": "Your heart Rate is not normal"
            }
        else:
            response = {
                "message": "NORMAL"
            }
            
    elif age >= 40 and age < 45:
        if value < 105 and value > 149:
            response = {
                "message": "Your heart Rate is not normal"
            }
        else:
            response = {
                "message": "NORMAL"
            }
            
    elif age >= 45 and age < 50:
        if value < 102 and value > 145:
            response = {
                "message": "Your heart Rate is not normal"
            }
        else:
            response = {
                "message": "NORMAL"
            }
            
    elif age >= 50 and age < 55:
        if value < 99 and value > 140:
            response = {
                "message": "Your heart Rate is not normal"
            }
        else:
            response = {
                "message": "NORMAL"
            }
            
    elif age >= 55 and age < 60:
        if value < 96 and value > 136:
            response = {
                "message": "Your heart Rate is not normal"
            }
        else:
            response = {
                "message": "NORMAL"
            }
            
    elif age >= 60 and age < 65:
        if value < 93 and value > 132:
            response = {
                "message": "Your heart Rate is not normal"
            }
        else:
            response = {
                "message": "NORMAL"
            }
            
    elif age >= 65 and age < 70:
        if value < 90 and value > 123:
            response = {
                "message": "Your heart Rate is not normal"
            }
        else:
            response = {
                "message": "NORMAL"
            }
    
    print("{} - Heart Rate: {}".format(LOGPREFIX, response))
    return response
    
def analyze_o2_level(value):
    print("{} - Analyze Oxygen Level".format(LOGPREFIX))
    print("{} - Payload: {}".format(LOGPREFIX, value))
    
    response = None

    if value >= 95:
        response = {
            "message": "NORMAL"
        }
    else:
        response = {
            "message": "Your blood oxygen level is not normal"
        }
    
    print("{} - Oxygen: {}".format(LOGPREFIX, response))
    return response
    
def analyze_respiratory_rate(value):
    print("{} - Analyze Respiratory Rate".format(LOGPREFIX))
    print("{} - Payload: {}".format(LOGPREFIX, value))
    
    response = None
    
    if value >= 12 and value <= 20:
        response = {
            "message": "NORMAL"
        }
    else:
        response = {
            "message": "Your respiratory rate is not normal"
        }
    
    print("{} - Respiratory Rate: {}".format(LOGPREFIX, response))
    return response
    
def get_age(userId):
    print("{} - Get Age".format(LOGPREFIX))
    print("{} - Payload: {}".format(LOGPREFIX, userId))
    
    age = 0
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    with Session(engine) as session:
        query= """
                SELECT DATEDIFF(YEAR,BIRTH_DT,GETDATE()) AS AGE
                FROM APP_USER WHERE ROLE_C = '1' AND USER_ID = :USER_ID
               """
                
        qs = session.execute(query, {
            "USER_ID": userId
        })
        
        for q in qs:
           age = q.AGE
    
    print("{} - Age: {}".format(LOGPREFIX, age))
    return age
    
def get_recommendation_record(userId):
    print("{} - Get Recommendation from MSSQL".format(LOGPREFIX))
    print("{} - Payload: {}".format(LOGPREFIX, userId))
    
    list_recommendation = []
    temp_list = []
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    with Session(engine) as session:
        query= """
               SELECT MSG_TP_T, MESSAGE_T FROM RECOMMENDATION 
               WHERE PATIENT_ID = :PATIENT_ID 
               ORDER BY CREATED_DT DESC;
               """
                
        qs = session.execute(query, {
            "PATIENT_ID": userId
        })
        
        for q in qs:
            msg = q.MESSAGE_T.strip()
            if msg in temp_list:
                continue
            
            temp_list.append(msg)
            
            data = {
                "MSG_TP_T": q.MSG_TP_T.strip(),
                "MESSAGE_T": msg
            }
            
            list_recommendation.append(data)
    
    print("{} - List Recommendation: {}".format(LOGPREFIX, list_recommendation))
    return list_recommendation

def insert_records(records):
    print("{} - Insert Records to MSSQL".format(LOGPREFIX))
    print("{} - Payload: {}".format(LOGPREFIX, records))
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    with Session(engine) as session:
        try: 
            for item in records:
                recommendation_id = session.execute(Sequence("RECOMMENDATION_SEQ"))
                print("{} - Recommendation ID: {}".format(LOGPREFIX, recommendation_id))
                item["ID"] = recommendation_id
                print("{} - Updated payload: {}".format(LOGPREFIX, item))
                row = mapping_RECOMMENDATION(item)
                session.add(row)
            session.commit()
        except Exception as e:
            print("{} - Exception: {}".format(LOGPREFIX, str(e)))
            session.rollback()
    
    print("{} - Insert Records to MSSQL ends.".format(LOGPREFIX))

def get_parameter_value(key):
    print("{} - Get Parameter key: {}".format(LOGPREFIX, key))
    ssm_client = boto3.client("ssm")
    value = ssm_client.get_parameter(Name=key, WithDecryption=True)
    # print("{} - key: {} - value: {}".format(LOGPREFIX, key, value.get("Parameter").get("Value")))
    return value.get("Parameter").get("Value")
    
def get_recommendation_graphdb(list_medical):
    print("{} - Get Recommendation from GraphDB".format(LOGPREFIX))
    
    list_recommendation = []
    
    query = """
            MATCH (disease:Disease {disease: 'XXX'})-->(precaution) RETURN precaution as text
            """
    
    # Set connection
    url = get_parameter_value("serverless-graphdb-url")
    user = get_parameter_value("serverless-graphdb-user")
    pwd = get_parameter_value("serverless-graphdb-pwd")
    
    driver = GraphDatabase.driver(url, auth=(user, pwd))
    
    session = driver.session()
    
    for condition in list_medical:
        
        result = session.run(query.replace('XXX', condition))
        resp = [item["text"] for item in result]
        if resp and len(resp) > 0:
            for item in resp:
                for key in item.keys(): 
                    list_recommendation.append(item.get(key))
    
    session.close()
    driver.close()
    
    print("{} - Recommendation: {}".format(LOGPREFIX, list_recommendation))
    return list_recommendation
    
class RECOMMENDATION(Base):
    __tablename__ = "RECOMMENDATION"
    
    ID = Column("ID", Integer, primary_key=True, autoincrement=False)
    PATIENT_ID = Column("PATIENT_ID", String)
    MESSAGE_T = Column("MESSAGE_T", String)
    MSG_TP_T = Column("MSG_TP_T", String)
    CREATED_DT = Column("CREATED_DT", DateTime)
    
def mapping_RECOMMENDATION(data):
    currentDateTime = datetime.now()
    
    return RECOMMENDATION(
        ID=data.get("ID"),
        PATIENT_ID=data.get("PATIENT_ID"),
        MESSAGE_T=data.get("MESSAGE_T"),
        MSG_TP_T=data.get("MSG_TP_T"),
        CREATED_DT=currentDateTime
        )