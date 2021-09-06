import pymongo
import boto3

from sqlalchemy.orm import declarative_base
from sqlalchemy.schema import Sequence
from sqlalchemy import Column, String, DateTime, Integer, Float, Date, MetaData
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.sql import select
from sqlalchemy import update, delete

from datetime import datetime, date
from json import dumps, loads


# Constants
LOGPREFIX = "myHealth"

# declarative base class
Base = declarative_base()

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

# Submit Vital Sign
def submit_vital_sign(event, context):
    print("{} - Submit Vital Sign".format(LOGPREFIX))
    user_id = ''
    
    # Get input parameter
    if event.get("pathParameters") and event.get("pathParameters").get("id"):
        user_id = event.get("pathParameters").get("id")
    else:
        return {"statusCode": 400, "body": "Missing User ID", "headers": {"Content-Type": "text/plain"}}
        
    payload = loads(event.get("body"))
    print("{} - id: {} and payload: {}".format(LOGPREFIX, user_id, payload))

    mdb_client = pymongo.MongoClient("mongodb+srv://myhealth-user:LKPnvE037i5ZLjc6@sg-cluster.8go0w.mongodb.net/admin?retryWrites=true&w=majority")
    print("DEBUG 1: {}".format(mdb_client))
    db = mdb_client['myhealth']
    print("DEBUG 2: {}".format(db))
    vitalsign = db['vitalsign']
    print("DEBUG 3: {}".format(vitalsign))
    vitalsign_id = vitalsign.insert_one(payload)
    print("id: {}".format(vitalsign_id))
    
    vitalsign_id = 'test'
        
    body = {
        "message": "Submit Vital Sign for {}".format(user_id),
        "id": vitalsign_id
    }
        
    response = {"statusCode": 200, "body": dumps(body), "headers": {"Content-Type": "application/json"}}
    
    return response
    
# Add User
def add_user(event, context):
    print("{} - Add User".format(LOGPREFIX))
    
    # Get input parameter
    payload = loads(event.get("body"))
    
    response_payload = {}
    
    if (payload):
        
        mssql_url = get_parameter_value('serverless-mssql-url')
        row = mapping_APP_USER(payload)
        engine = create_engine(mssql_url)

        # create session and add objects
        with Session(engine) as session:
            user_id = session.execute(Sequence("APP_USER_SEQ"))
            
            row.USER_ID = user_id
            session.add(row)
            session.commit()
            
            response_payload = {
                "USER_ID": user_id
            }
            
    else:
        response_payload = {
            "message": "Missing Payload"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json"}}
        
    return {"statusCode": 200, "body": dumps(response_payload), "headers": {"Content-Type": "application/json"}}
    
# Update User Detail
def update_user(event, context):
    print("{} - Update User".format(LOGPREFIX))
    
    response_payload = {}
    
    # Get input parameter
    user_id = None
    if event.get("pathParameters") and event.get("pathParameters").get("userId"):
        user_id = event.get("pathParameters").get("userId")
    else:
        response_payload = {
            "message": "Missing User ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json"}}
        
    payload = loads(event.get("body"))
    
    if (payload):
        
        mssql_url = get_parameter_value('serverless-mssql-url')
        row = mapping_APP_USER(payload)
        engine = create_engine(mssql_url)

        # create session and update objects
        with Session(engine) as session:
            stmt = update(APP_USER).where(APP_USER.USER_ID == user_id).values(payload).execution_options(synchronize_session="fetch")
            session.execute(stmt)
            session.commit()
            
            response_payload = {
                "USER_ID": user_id
            }
            
    else:
        response_payload = {
            "message": "Missing Payload"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json"}}
        
    return {"statusCode": 200, "body": dumps(response_payload), "headers": {"Content-Type": "application/json"}}
    
# Delete user
def delete_user(event, context):
    print("{} - Delete User".format(LOGPREFIX))
    
     # Get input parameter
    user_id = None
    if event.get("pathParameters") and event.get("pathParameters").get("userId"):
        user_id = event.get("pathParameters").get("userId")
    else:
        response_payload = {
            "message": "Missing User ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json"}}
        
    if (user_id):
        mssql_url = get_parameter_value('serverless-mssql-url')
        engine = create_engine(mssql_url)
        
        with Session(engine) as session:
            stmt = delete(APP_USER).where(APP_USER.USER_ID == user_id).execution_options(synchronize_session="fetch")
            session.execute(stmt)
            session.commit()
            
    else:
        response_payload = {
            "message": "Missing ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json"}}
        
    return {"statusCode": 204}

# Get User detail
def get_user(event, context):
    print("{} - Get User".format(LOGPREFIX))
    
    response_payload = {}
    
    # Get input parameter
    user_id = None
    if event.get("pathParameters") and event.get("pathParameters").get("email"):
        user_id = event.get("pathParameters").get("email")
    else:
        response_payload = {
            "message": "Missing User ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json"}}
        
    if (user_id):
        mssql_url = get_parameter_value('serverless-mssql-url')
        engine = create_engine(mssql_url)
        
        with Session(engine) as session:
            qs = session.query(APP_USER).filter(APP_USER.EMAIL == user_id)
            
            for q in qs:
            
                response_payload = {
                    "USER_ID": q.USER_ID.strip() if q.USER_ID else "",
                    "FULL_NAME": q.FULLNAME.strip() if q.FULLNAME else "",
                    "BIRTH_DT": q.BIRTH_DT.strftime("%d %B %Y") if q.BIRTH_DT else "",
                    "ROLE_C": q.ROLE_C if q.ROLE_C else "",
                    "GENDER_C": q.GENDER_C if q.GENDER_C else "",
                    "ADDRESS": q.ADDRESS.strip() if q.ADDRESS else "",
                    "EMAIL": q.EMAIL.strip() if q.EMAIL else "",
                    "PHONE": q.PHONE.strip() if q.PHONE else ""
                }
            
            
    else:
        response_payload = {
            "message": "Missing User ID"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json"}}
        
    return {"statusCode": 200, "body": dumps(response_payload, default=str), "headers": {"Content-Type": "application/json"}}
    
# Search doctor
def search_doctor(event, context):
    print("{} - Search Doctor".format(LOGPREFIX))
    
    list_doctor = []
    keyword_specialty = ''
    keyword_name = ''
    
    # Get input parameter
    if event.get("queryStringParameters"): 
        if event.get("queryStringParameters").get("specialty"):
            keyword_specialty = event.get("queryStringParameters").get("specialty")
        
        if event.get("queryStringParameters").get("name"):
            keyword_name = event.get("queryStringParameters").get("name")
            
    # Construct SQL Query
    final_query = ""
    main_query = """
                SELECT A.USER_ID, A.FULLNAME, A.BIRTH_DT, D.GENDER_T, A.EMAIL, A.ADDRESS, A.PHONE, C.SPECIALTY_T
                FROM APP_USER A, DOCTOR_SPECIALTY B, SPECIALTY C, GENDER D
                WHERE A.USER_ID = B.DOCTOR_C 
                AND B.SPECIALTY_C = C.SPECIALTY_C
                AND A.GENDER_C = D.GENDER_C     
                AND A.ROLE_C = 2
                """
    
    if (keyword_specialty and keyword_name):
    
        specialty_query = """
                      AND A.USER_ID IN (SELECT DOCTOR_C FROM DOCTOR_SPECIALTY M, SPECIALTY N 
                      WHERE M.SPECIALTY_C = N.SPECIALTY_C AND N.SPECIALTY_T = '{}')
                      """.format(keyword_specialty)
            
        fullname_query = """
                     AND A.FULLNAME LIKE '%{}%'
                     """.format(keyword_name)
        
        final_query = "{} {} {}".format(main_query, specialty_query, fullname_query)

    elif keyword_specialty:
        
        specialty_query = """
                      AND A.USER_ID IN (SELECT DOCTOR_C FROM DOCTOR_SPECIALTY M, SPECIALTY N 
                      WHERE M.SPECIALTY_C = N.SPECIALTY_C AND N.SPECIALTY_T = '{}')
                      """.format(keyword_specialty)
    
        final_query = "{} {}".format(main_query, specialty_query)
        
    elif keyword_name:
        fullname_query = """
                     AND A.FULLNAME LIKE '%{}%'
                     """.format(keyword_name)
        
        final_query = "{} {}".format(main_query, fullname_query)
        
    else:
        final_query = main_query
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    with Session(engine) as session:
        qs = session.execute(final_query)
    
        for q in qs:
            doctor = {
                "USER_ID": q.USER_ID.strip() if q.USER_ID else "",
                "FULL_NAME": q.FULLNAME.strip() if q.FULLNAME else "",
                "BIRTH_DT": q.BIRTH_DT.strftime("%d %B %Y") if q.BIRTH_DT else "",
                "GENDER_T": q.GENDER_T.strip() if q.GENDER_T else "",
                "ADDRESS": q.ADDRESS.strip() if q.ADDRESS else "",
                "EMAIL": q.EMAIL.strip() if q.EMAIL else "",
                "PHONE": q.PHONE.strip() if q.PHONE else "",         
                "SPECIALTY": []
            }
            specialty = q.SPECIALTY_T.strip()
            
            temp = {val.get("USER_ID"): val for val in list_doctor}
            
            if doctor.get("USER_ID") not in temp.keys():
                doctor.get('SPECIALTY').append(specialty)
                list_doctor.append(doctor)
            else:            
                doctor = temp.get(doctor.get("USER_ID"))
                doctor.get('SPECIALTY').append(specialty)
    
    return {"statusCode": 200, "body": dumps(list_doctor, default=str), "headers": {"Content-Type": "application/json"}}

# Search doctor
def search_patient(event, context):
    print("{} - Search Patient".format(LOGPREFIX))
    
    list_patient = []
    keyword_name = ''
    
    # Get input parameter
    if event.get("queryStringParameters") and event.get("queryStringParameters").get("name"):
        keyword_name = event.get("queryStringParameters").get("name")
        
    if keyword_name == '':
        response_payload = {
            "message": "Missing name keyword"
        }
        return {"statusCode": 400, "body": dumps(response_payload), "headers": {"Content-Type": "application/json"}}
            
    # Construct SQL Query
    final_query = """
                SELECT A.USER_ID, A.FULLNAME, A.BIRTH_DT, B.GENDER_T, A.EMAIL, A.ADDRESS, A.PHONE
                FROM APP_USER A, GENDER B
                WHERE A.GENDER_C = B.GENDER_C   
                AND A.ROLE_C = 1
                AND A.FULLNAME LIKE '%{}%'
                """.format(keyword_name.upper())
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    with Session(engine) as session:
        qs = session.execute(final_query)
    
        for q in qs:
            patient = {
                "USER_ID": q.USER_ID.strip() if q.USER_ID else "",
                "FULL_NAME": q.FULLNAME.strip() if q.FULLNAME else "",
                "BIRTH_DT": q.BIRTH_DT.strftime("%d %B %Y") if q.BIRTH_DT else "",
                "GENDER_T": q.GENDER_T.strip() if q.GENDER_T else "",
                "ADDRESS": q.ADDRESS.strip() if q.ADDRESS else "",
                "EMAIL": q.EMAIL.strip() if q.EMAIL else "",
                "PHONE": q.PHONE.strip() if q.PHONE else ""
            }
            
            list_patient.append(patient)
        
    return {"statusCode": 200, "body": dumps(list_patient, default=str), "headers": {"Content-Type": "application/json"}}
    
# Get list of specialty
def get_specialty(event, context):
    print("{} - Search Patient".format(LOGPREFIX))
    
    list_specialty = []
    
    mssql_url = get_parameter_value('serverless-mssql-url')
    engine = create_engine(mssql_url)
    
    with Session(engine) as session:
        qs = session.query(SPECIALTY)
        
        for q in qs:
            specialty = {
                "SPECIALTY_T": q.SPECIALTY_T.strip(),
                "SPECIALTY_C": q.SPECIALTY_C
            }
            
            list_specialty.append(specialty)
            
    return {"statusCode": 200, "body": dumps(list_specialty, default=str), "headers": {"Content-Type": "application/json"}}
    
# Common functions
def get_parameter_value(key):
    print("{} - Get Parameter key: {}".format(LOGPREFIX, key))
    ssm_client = boto3.client("ssm")
    value = ssm_client.get_parameter(Name=key, WithDecryption=False)
    return value.get("Parameter").get("Value")


# Table SPECIALTY
class SPECIALTY(Base):
    __tablename__ = 'SPECIALTY'
    
    SPECIALTY_C = Column("SPECIALTY_C", String, primary_key=True)
    SPECIALTY_T = Column("SPECIALTY_T", String)
    
# Table APP_USER
class APP_USER(Base):
    __tablename__ = 'APP_USER'

    USER_ID = Column("USER_ID", String, primary_key=True)
    FULLNAME = Column("FULLNAME", String)
    BIRTH_DT = Column("BIRTH_DT", Date)
    ROLE_C = Column("ROLE_C", Integer)
    GENDER_C = Column("GENDER_C", Integer)
    ADDRESS = Column("ADDRESS", String)
    EMAIL = Column("EMAIL", String)
    PHONE = Column("PHONE", String)
    CREATED_DT = Column("CREATED_DT", DateTime)
    UPDATED_DT = Column("UPDATED_DT", DateTime)
    
# Mapping data to APP_USER
def mapping_APP_USER(data):
    currentDateTime = datetime.now()
    return APP_USER(
        FULLNAME = data.get("FULLNAME"),
        BIRTH_DT = data.get("BIRTH_DT"),
        ROLE_C = data.get("ROLE_C"),
        GENDER_C = data.get("GENDER_C"),
        ADDRESS = data.get("ADDRESS"),
        EMAIL = data.get("EMAIL"),
        PHONE = data.get("PHONE"),
        CREATED_DT = currentDateTime,
        UPDATED_DT = currentDateTime
    )
    