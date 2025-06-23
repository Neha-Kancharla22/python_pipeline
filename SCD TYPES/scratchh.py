import pyodbc

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-A4K6TVV;'
    'DATABASE=pipelines;'
    'UID=sa;'
    'PWD=admin@123'
)
cursor = conn.cursor()
