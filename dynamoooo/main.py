from extract import connect_to_dynamo,connect_to_mongodb,extract_documentt
from load4 import load_to_dynamodb
from transform import load_to_target_table
def main():
    '''db=connect_to_mongodb()
    print("connected to mongodb")

    df=extract_documentt()
    print("data is extracted from collection")

    dynamo_table=connect_to_dynamo()
    print("connected to dynamodb")

    load_to_dynamodb(df, dynamo_table)
    print(" loaded into dynamo_table")'''
    load_to_target_table()


if __name__ == '__main__':
    main()
