import pyodbc
conn=pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-A4K6TVV;'
    'DATABASE=pipelines;'
    'UID=sa;'
    'PWD=admin@123'
)
cursor = conn.cursor()
create_table_query='''CREATE TABLE CUSTOMERS1(Customer_id INT PRIMARY KEY,
    Name NVARCHAR(100),
    Email NVARCHAR(100),
    Address NVARCHAR(100),
    Phone NVARCHAR(50))'''
cursor.execute(create_table_query)
conn.commit()

