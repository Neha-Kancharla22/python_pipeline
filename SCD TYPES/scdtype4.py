import pyodbc
from datetime import datetime
conn=pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-A4K6TVV;'
    'DATABASE=pipelines;'
    'UID=sa;'
    'PWD=admin@123'
)
cursor= conn.cursor()
#create_table_query4='''CREATE TABLE CUSTOMERS_HISTORY(row_id INT IDENTITY(1,1) PRIMARY KEY,Customer_id INT ,
    #Name NVARCHAR(100),
    #Email NVARCHAR(100),
    #Address NVARCHAR(100),
    #Phone NVARCHAR(50))'''
#cursor.execute(create_table_query4)

#cursor.execute('''ALTER TABLE CUSTOMERS_HISTORY
               #ADD start_date datetime , End_date datetime''')

#create_table_query5='''CREATE TABLE CUSTOMERS_4(row_id INT IDENTITY(1,1) PRIMARY KEY,Customer_id INT ,
    #Name NVARCHAR(100),
    #Email NVARCHAR(100),
    #Address NVARCHAR(100),
    #Phone NVARCHAR(50))'''
#cursor.execute(create_table_query5)

#customer_data =[(1,'Michelle Kidd','vayala@example.net','USNS Santiago, FPO AE 80872','619-723-4258'),
              #(2,'Brad Newton','taylorcatherine@example.net','38783 Oliver Street, West Kristenborough,MT 99752','537-674-1158'),
              #(3,'Larry Torres','dsanchez@example.net','6845 Steele Turnpike, West Erikabury, UT 37487','810-256-4505'),
               #(4,'Kimberly Price','jessicaknight@example.com','1631 Alexis Meadows, Lake Amanda, CA 75179','423-222-9779'),
              #(5,'Matthew Phillips','qwilliams@example.com','2274 Williams Heights Suite 895, Andersonhaven, OR 80565','220-763-3522')]

#cursor.executemany(
   #'''INSERT INTO CUSTOMERS_4 (Customer_id, Name, Email, Address, Phone) VALUES (?, ?, ?, ?, ?)''',
    #customer_data)

#cursor.execute('''SELECT * FROM CUSTOMERS_4 WHERE Customer_id=?''',(1,))
#current=cursor.fetchone()

#cursor.execute('''INSERT INTO CUSTOMERS_HISTORY(Customer_id,Name,Email,Address,Phone,start_date,End_date) VALUES(?,?,?,?,?,?,?)''',
               #(current.Customer_id,current.Name,current.Email,current.Address,current.Phone,
                #datetime.strptime("10-JUN-2023", "%d-%b-%Y"),
                #datetime.strptime("23-JUN-2025", "%d-%b-%Y")))

#cursor.execute('''UPDATE CUSTOMERS_4 SET Address='Burj Khalifa,Dubai' WHERE Customer_id=?''',(1) )

cursor.execute(''' INSERT INTO CUSTOMERS_HISTORY (Customer_id,Name,Email,Address,Phone,start_date,End_date) VALUES(?,?,?,?,?,getdate(),NULL)''',
               (1,'Michelle Kidd','vayala@example.net','Burj Khalifa,Dubai','619-723-4258'))
                

conn.commit()
