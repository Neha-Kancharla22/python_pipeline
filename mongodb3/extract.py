import json
import configparser
import pandas as pd
from sqlalchemy import create_engine
import urllib
from pymongo import MongoClient


'''def load_json_to_mongodb(json_path, db_name, collection_name):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["collection"]
    collection = db["project2"]

    with open(json_path, 'r') as f:
        file_data = json.load(f)

    if isinstance(file_data, list):
        collection.insert_many(file_data)
        print(f"Inserted {len(file_data)} documents into {db_name}.{collection_name}")
    else:
        collection.insert_one(file_data)
        print(f" Inserted one document into {db_name}.{collection_name}")

    client.close()'''

def connect_to_mongodb():
    client = MongoClient("mongodb://localhost:27017")
    db = client["collection"]
    collection = db["project2"]
    print("python-mongodb success")
    return collection

def read_config(file_path=r'C:\Users\Neha\Documents\mongoo1\config\config.ini'):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config


def connect_sql_server(config):
    config = read_config()
    print("Config sections found:", config.sections())
    sql_config = config['SQL_SERVER']

    conn_str = urllib.parse.quote_plus(
        f"DRIVER={sql_config['driver']};"
        f"SERVER={sql_config['host']};"
        f"DATABASE={sql_config['database']};"
        f"UID={sql_config['username']};"
        f"PWD={sql_config['password']}"
    )

    return create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")


def extract_documents():
    collection=connect_to_mongodb()
    documents = list(collection.find())  
    df = pd.DataFrame(documents)
    print(df)
    return df
    

if __name__ == "__main__":
    config=read_config()
    connect_sql_server(config)





