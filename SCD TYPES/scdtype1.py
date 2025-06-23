import pyodbc
conn=pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-A4K6TVV;'
    'DATABASE=pipelines;'
    'UID=sa;'
    'PWD=admin@123'
)
cursor = conn.cursor()

customers1_data =[(1,'Michelle Kidd','vayala@example.net','USNS Santiago, FPO AE 80872','619-723-4258'),
              (2,'Brad Newton','taylorcatherine@example.net','38783 Oliver Street, West Kristenborough,MT 99752','537-674-1158'),
              (3,'Larry Torres','dsanchez@example.net','6845 Steele Turnpike, West Erikabury, UT 37487','810-256-4505'),
               (4,'Kimberly Price','jessicaknight@example.com','1631 Alexis Meadows, Lake Amanda, CA 75179','423-222-9779'),
              (5,'Matthew Phillips','qwilliams@example.com','2274 Williams Heights Suite 895, Andersonhaven, OR 80565','220-763-3522')]

cursor.executemany(
   '''INSERT INTO CUSTOMERS1 (Customer_id, Name, Email, Address, Phone) VALUES (?, ?, ?, ?, ?)''',
    customers1_data)

#scd type1
#updating the email,address
cursor.execute('''
    UPDATE customers1
    SET  email = ?, address = ?
    WHERE customer_id = ?
''', ( 'Michellekidd@example.com', 'Dubai', 1))
conn.commit()
















