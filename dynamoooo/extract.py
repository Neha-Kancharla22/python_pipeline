import boto3
from pymongo import MongoClient
import pandas as pd
import configparser
from sqlalchemy import create_engine
import urllib


def connect_to_mongodb():
    client = MongoClient("mongodb://localhost:27017")
    db = client["collection"]
    print("python-mongodb success")
    return db["project1"]


def extract_documentt():
    collection = connect_to_mongodb()
    documents = list(collection.find())
    df = pd.DataFrame(documents)

    if '_id' in df.columns:
        df.drop(columns=['_id'], inplace=True)
    print(df.head())
    return df


def connect_to_dynamo():
    dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')  
    table = dynamodb.Table('project_table') 
    return table

def read_config(file_path=r'C:\Users\Neha\Documents\dynamodb1\config.config\config.ini'):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

def connect_sql_server(config):
    sql_config = config['SQL_SERVER']

    conn_str = urllib.parse.quote_plus(
        f"DRIVER={sql_config['driver']};"
        f"SERVER={sql_config['host']};"
        f"DATABASE={sql_config['database']};"
        f"UID={sql_config['username']};"
        f"PWD={sql_config['password']}"
    )

    return create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")