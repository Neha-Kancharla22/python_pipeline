from transform import transform_documents
import pandas as pd
from extract import connect_sql_server,read_config

def load_to_sql():
    df=transform_documents()
    config=read_config()
    connect=connect_sql_server(config)
    df['_id'] = df['_id'].astype(str)  
    df.to_sql('projectss',con=connect,if_exists='replace', index=False)
    print("loading is completed")



