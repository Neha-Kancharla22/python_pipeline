import configparser
import pandas as pd
from sqlalchemy import create_engine
import logging
import os
from urllib.parse import quote_plus  
 
def get_mysql_engine():
    """
    Establish connection to MySQL using SQLAlchemy engine.
    Reads configuration from config.ini.
    """
    config = configparser.ConfigParser()
    config.read(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini')))
 
    user = config['MYSQL']['user']
    password = quote_plus(config['MYSQL']['password'])  
    host = config['MYSQL']['host']
    port = config['MYSQL']['port']
    database = config['MYSQL']['database']
 
    connection_url = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_url)
    return engine
 
def loadtomysql(df, table_name):
    """
    Load DataFrame into MySQL table.
    Creates the table if it does not exist.
    Appends rows otherwise.
    """
    engine = get_mysql_engine()
    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        logging.info(f"Loaded {len(df)} rows into table: {table_name}")
        print(f"Loaded {len(df)} rows into table: {table_name}")
    except Exception as e:
        logging.error(f"Failed to load data into {table_name}: {str(e)}")
        print(f"Load failed: {str(e)}")
 