from extract import connect_to_dynamo,connect_to_mongodb,read_config,connect_sql_server
from transform import extract_and_sort,connect_to_target_table
import json
from decimal import Decimal
import pandas as pd

def load_to_dynamodb(df, table):
    for index, row in df.iterrows():
        item = {col: str(row[col]) for col in df.columns} 
        table.put_item(Item=item)
    print("Data loaded into DynamoDB!")



def load_to_ssms():
    config=read_config()
    connect=connect_sql_server(config)
    df=extract_and_sort(sort_by='start_date') 
    if df['technologies'].dtype == 'object':
        df['technologies'] = df['technologies'].apply(lambda x: x.split(',') if isinstance(x, str) else x)
    df = df.explode('technologies')
    df['technologies'] = df['technologies'].str.strip()
    df.to_sql(name='dynamo1',con=connect,if_exists='replace',index=False)
    print("data loaded")


