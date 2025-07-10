from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
import boto3
import os
import pyodbc
import configparser
from dotenv import load_dotenv

def authenticate_gmail(credentials_path=r'C:\Users\Neha\Downloads\client_secret_987908746180-l7h9doulmpjrc6n44i515e9elhniaa5u.apps.googleusercontent.com.json'):
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
    creds = flow.run_local_server(port=0)
    service = build('gmail', 'v1', credentials=creds)
    return service


def load_aws_config(env_file=r'C:\Users\Neha\Documents\s32\config\config.env'):
    load_dotenv(env_file)  # Load variables from .env

    config = {
        'access_key': os.getenv('AWS_ACCESS_KEY_ID'),
        'secret_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'region': os.getenv('AWS_REGION'),
        'bucket': os.getenv('BUCKET_NAME')
    }

    if not all(config.values()):
        raise ValueError("Missing one or more AWS configuration variables")

    return config

def connect_to_s3():
    aws = load_aws_config()

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws['access_key'],
        aws_secret_access_key=aws['secret_key'],
        region_name=aws['region']
    )

    print(f"Connected to S3 bucket: {aws['bucket']}")
    return s3, aws['bucket']


def read_configg(filepath=r'C:\Users\Neha\Documents\emailll\config\config.ini'):
    config=configparser.ConfigParser()
    config.read(filepath)
    print(f"Reading from: {filepath}")
    print("print the sections:",config.sections())
    return config
    
def connect_to_SQL(config):
    sql_config = config['SQL_SERVER']
 
    conn_str = (
        f"DRIVER={sql_config['driver']};"
        f"SERVER={sql_config['server']};"
        f"DATABASE={sql_config['database']};"
        f"UID={sql_config['username']};"
        f"PWD={sql_config['password']}"
    )
 
    return pyodbc.connect(conn_str)

















