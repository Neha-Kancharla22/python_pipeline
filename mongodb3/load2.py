from transform import transform_data
from extract import connect_sql_server, read_config
import pandas as pd
 
def load_to_sql():
    df = transform_data()
    config = read_config()
    engine = connect_sql_server(config)
 
    if '_id' in df.columns:
        df['_id'] = df['_id'].astype(str)
 
    df.to_sql('project_combined', con=engine, if_exists='replace', index=False)
    print(" All project data loaded into one table.")
 
 
   
   
 