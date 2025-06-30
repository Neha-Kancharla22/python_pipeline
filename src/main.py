# main.py
import logging
import configparser
import os
import pandas as pd
from extract import extract_data
from transform import transform_scd1, transform_scd2, transform_scd3
from load1 import loadtomysql, get_mysql_engine
 
def setup_logging():
    """Set up logging configuration from config.ini."""
    config = configparser.ConfigParser()
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini'))
    config.read(config_path)
    print("🔍 Parsed MySQL password:", config['MYSQL']['password'])
 
 
    #showing the contents of config file
    with open(config_path, 'r') as f:
        print("CONFIG FILE CONTENT:")
        print(f.read())
 
    print("Loaded config sections:", config.sections())  # Debug print
 
    logging.basicConfig(
        filename=config['ETL']['log_file'],
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
 
def get_existing_data(table_name):
    """Load existing data from MySQL target table if available."""
    engine = get_mysql_engine()
    try:
        return pd.read_sql(f"SELECT * FROM {table_name}", engine)
    except Exception as e:
        logging.warning(f"Could not read from {table_name}: {str(e)}")
        return pd.DataFrame()
 
def run_etl():
    """Run the full ETL process for SCD types 1, 2, and 3."""
    setup_logging()
    logging.info(" ETL process started")
 
    #Extract 
    df = extract_data()
 
    # Load existing data 
    df_scd2_existing = get_existing_data("customers_scd2")
    df_scd3_existing = get_existing_data("customers_scd3")
 
    #Transforming using each SCD type
    df_scd1 = transform_scd1(df)
    df_scd2 = transform_scd2(df, df_scd2_existing)
    df_scd3 = transform_scd3(df, df_scd3_existing)
 
    #Loading into corresponding MySQL tables
    print("df_scd1 rows:", len(df_scd1))
    loadtomysql(df_scd1, "customers_scd1")
    print("df_scd2 rows:", len(df_scd2))
    loadtomysql(df_scd2, "customers_scd2")
    print("df_scd3 rows:", len(df_scd3))
    loadtomysql(df_scd3, "customers_scd3")
 
    logging.info("ETL process completed successfully")
 
if __name__ == "__main__":
    run_etl()
 
 

 
 