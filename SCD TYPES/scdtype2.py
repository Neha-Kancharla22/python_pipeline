import pyodbc

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-A4K6TVV;'
    'DATABASE=pipelines;'
    'UID=sa;'
    'PWD=admin@123'
)
cursor = conn.cursor()

#dropping the primary key for customer_id
cursor.execute('''ALTER TABLE customers1
DROP CONSTRAINT PK__CUSTOMER__8CB382B1AD91A226''')

#adding a column id with primary key constraint
cursor.execute('''ALTER TABLE customers1
ADD id INT IDENTITY(1,1) PRIMARY KEY;''')

#updating again because it was updated in scd type 1
cursor.execute('''
   UPDATE CUSTOMERS1
   SET Email='vayala@example.net',
   Address='USNS Santiago, FPO AE 80872'        
   WHERE Customer_id=1''')

#adding columns in the table customers1
cursor.execute('''
  ALTER TABLE CUSTOMERS1
  ADD Start_Date DATE DEFAULT GETDATE(),
  End_Date DATE NULL,
  Is_Current VARCHAR(10) DEFAULT 'Active';''')

#updating the existing rows values in customers1
cursor.execute('''
    UPDATE CUSTOMERS1
    SET Start_Date = COALESCE(Start_Date, GETDATE()),
    End_Date = COALESCE(End_Date, NULL),
    Is_Current = COALESCE(Is_Current, 'Active');''')

#updating the row 1 to make it inactive 
cursor.execute('''
    UPDATE CUSTOMERS1
    SET End_Date = GETDATE(),
    Is_Current='Inactive'
    WHERE Customer_id=1''')

#inserting the new roww for the active address and email for customer_id=1
cursor.execute('''
              INSERT INTO CUSTOMERS1 (Customer_id, Name, Email, Address, Phone,Start_Date,End_Date,Is_Current) VALUES (?, ?, ?, ?, ?, GETDATE(), NULL, ?)
               ''',(1,'Michelle Kidd','Michellekidd@example.com',' Burj Khalifa,Floor no;108 AE 80872','619-723-4258','Active' ) )

               
conn.commit()

