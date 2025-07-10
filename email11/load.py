from connect import connect_to_SQL,read_configg

def insert_sql(email_data,conn):
    
    cursor=conn.cursor()

    attachment_1=email_data['attachments'][0] if len(email_data['attachments'])>0 else None
    attachment_2=email_data['attachments'][1] if len(email_data['attachments'])>1 else None

    cursor.execute('''INSERT INTO Email_communications2(sender_name,receiver_name,cc,subject,body,attachment_1_url,attachment_2_url) VALUES(?,?,?,?,?,?,?)''',
                   email_data['sender_name'],email_data['receiver_name'],email_data['cc'],email_data['subject'],email_data['body'],
                   attachment_1,attachment_2)
    conn.commit()
    cursor.close()
