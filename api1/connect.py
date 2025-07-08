import pyodbc
import urllib
import configparser
from sqlalchemy import create_engine
 
 
#sql server connect
def read_config(file_path=r'C:\Users\Neha\Documents\fastapi1\config.config\config.ini'):
    config = configparser.ConfigParser()
    config.read(file_path)
    print("Loaded sections:", config.sections())
    return config
 
def get_db_connection(config):
    sql_config = config['SQL_SERVER']
 
    conn_str = (
        f"DRIVER={sql_config['driver']};"
        f"SERVER={sql_config['server']};"
        f"DATABASE={sql_config['database']};"
        f"UID={sql_config['username']};"
        f"PWD={sql_config['password']}"
    )
 
    return pyodbc.connect(conn_str)
 
 