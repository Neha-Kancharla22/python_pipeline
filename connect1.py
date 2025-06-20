import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus  
server = 'DESKTOP-A4K6TVV'     
database = 'pipelines'                     
username = 'sa'                            
password = 'admin@123'                    
driver = 'ODBC Driver 17 for SQL Server'   
encoded_password = quote_plus(password)
connection_string = (
    f"mssql+pyodbc://{username}:{encoded_password}@{server}/{database}"
    f"?driver={quote_plus(driver)}"
)
engine = create_engine(connection_string)

df = pd.read_csv(r'C:\Users\Neha\Downloads\us_customer_data 1.csv')
df.to_sql('customers', con=engine, if_exists='replace', index=False)

df = pd.read_csv(r'C:\Users\Neha\Downloads\order_data 1.csv')
df.to_sql('orders', con=engine, if_exists='replace', index=False)

print(" CSV data imported into SQL Server using SQL authentication.")

customers_df = pd.read_sql("SELECT * FROM customers", con=engine)
orders_df = pd.read_sql("SELECT * FROM orders", con=engine)
 