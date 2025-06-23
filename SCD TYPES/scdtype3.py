import pyodbc
conn=pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-A4K6TVV;'
    'DATABASE=pipelines;'
    'UID=sa;'
    'PWD=admin@123'
)
cursor = conn.cursor()
create_table_query2='''CREATE TABLE CUSTOMERS2(row_id INT IDENTITY(1,1) PRIMARY KEY,Customer_id INT ,
    Name NVARCHAR(100),
    Email NVARCHAR(100),
    Address NVARCHAR(100),
    Phone NVARCHAR(50))'''
cursor.execute(create_table_query2)
customers1_data =[(1,'Michelle Kidd','vayala@example.net','USNS Santiago, FPO AE 80872','619-723-4258'),
            (2,'Brad Newton','taylorcatherine@example.net','38783 Oliver Street, West Kristenborough,MT 99752','537-674-1158'),
            (3,'Larry Torres','dsanchez@example.net','6845 Steele Turnpike, West Erikabury, UT 37487','810-256-4505'),
            (4,'Kimberly Price','jessicaknight@example.com','1631 Alexis Meadows, Lake Amanda, CA 75179','423-222-9779'),
            (5,'Matthew Phillips','qwilliams@example.com','2274 Williams Heights Suite 895, Andersonhaven, OR 80565','220-763-3522')]

cursor.executemany(
   '''INSERT INTO CUSTOMERS2 (Customer_id, Name, Email, Address, Phone) VALUES (?, ?, ?, ?, ?)''',
    customers1_data)

cursor.execute('''ALTER TABLE CUSTOMERS2
  ADD PREVIOUS_EMAIL NVARCHAR(200),PREVIOUS_ADDRESS NVARCHAR(300)''')

cursor.execute('''UPDATE CUSTOMERS2 SET PREVIOUS_EMAIL=Email,PREVIOUS_ADDRESS=Address
               WHERE Customer_id=?''',(1,))
cursor.execute('''
    UPDATE CUSTOMERS2
    SET Email= ?, 
        Address = ?
    WHERE Customer_id = ?
''', ('michallekidd@example.com', 'Burj Khalifa,floor no:106,Dubai, AE 80872', 1))
 
conn.commit()




