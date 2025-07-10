from connect import authenticate_gmail,connect_to_SQL,read_configg,connect_to_s3
from extract import fetch_emails, parse_email
from load import insert_sql

def main():
    #firstly creating the gmail api service which authenticates the user to access the mails 
    gmail_service = authenticate_gmail()
    #fetching the mails by the api service 
    messages = fetch_emails(gmail_service)
    #reading config.ini file 
    config=read_configg()
    #connecting to sql
    conn=connect_to_SQL(config)
    #connecting to s3 bucket
    s3, bucket = connect_to_s3()
    #inserting data into the sql server and uploading the attachments into the s3 bucket
    for msg in messages:
        email_data = parse_email(gmail_service, msg['id'],s3, bucket)
        insert_sql(email_data,conn)
    #data is inserted
    conn.close()
    #connections are closed
if __name__ == "__main__":
    main()