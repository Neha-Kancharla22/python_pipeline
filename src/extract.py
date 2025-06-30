import pyodbc
import configparser
import logging
import pandas as pd
import os
 
def extract_data(last_load_date=None):
    """Extract customer data from SQL Server with optional incremental load."""
    #config file
    config = configparser.ConfigParser()
    config.read(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini')))
 
 
    #SQL Server connection
    conn = pyodbc.connect(
        f"DRIVER={{{config['SQL_SERVER']['driver']}}};"
        f"SERVER={config['SQL_SERVER']['host']};"
        f"DATABASE={config['SQL_SERVER']['database']};"
        f"UID={config['SQL_SERVER']['user']};"
        f"PWD={config['SQL_SERVER']['password']}"
    )
 
    # query
    query = "SELECT * FROM CUSTOMER"
    if last_load_date:
        query += " WHERE last_modified > ?"
        df = pd.read_sql(query, conn, params=[last_load_date])
    else:
        df = pd.read_sql(query, conn)
 
    #  Closing connection,log
    conn.close()
    logging.info(f"Extracted {len(df)} rows from customers_info")
    return df
 