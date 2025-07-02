from extract import connect_to_dynamo
import pandas as pd
import boto3

def extract_and_sort(sort_by='start_date', ascending=True):
    table = connect_to_dynamo()
    items = []
    response = table.scan()
    items.extend(response['Items'])
 
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])
 
    df = pd.DataFrame(items)
 
    if sort_by in df.columns:
        df.sort_values(by=sort_by, ascending=ascending, inplace=True)
    else:
        print(f"Column '{sort_by}' not found in table. Returning unsorted data.")
 
    return df


def connect_to_target_table():
    dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
    return dynamodb.Table('target2_table')










