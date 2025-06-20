import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus  
import  re
import random
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
 
 #extracting customers and order datasets
customers_df = pd.read_sql("SELECT * FROM customers", con=engine)
orders_df = pd.read_sql("SELECT * FROM orders", con=engine)
 
# removing prefixes and suffixes and cleaning
def clean_and_split_name(name):
    prefixes = ['Mr.', 'Mrs.', 'Miss', 'Dr.']
    suffixes = ['Jr.', 'Sr.', 'II', 'III']
    for prefix in prefixes:
        name = name.replace(prefix, '')
    for suffix in suffixes:
        name = name.replace(suffix, '')
    name = name.strip()
    parts = name.split()
    return pd.Series([parts[0], ' '.join(parts[1:]) if len(parts) > 1 else ''])
 
customers_df[['First Name', 'Last Name']] = customers_df['name'].astype(str).apply(clean_and_split_name)
customers_df['name'] = customers_df['First Name'].str.title() + ' ' + customers_df['Last Name'].str.title()
customers_df['email'] = customers_df['email'].fillna((customers_df['First Name'] + customers_df['Last Name']).str.lower() + '@example.com')

#filling missing phone numbers with random numbers by using function

def generate_p():
    return f"{random.randint(7000000000, 9999999999)}"

#country code
customers_df['country_code'] = customers_df['address'].str.slice(-8, -6)

# dialing code
dialing_df=pd.read_csv(r"C:\Users\Neha\Downloads\Country-codes.csv",encoding='ISO-8859-1')
dialing_df.rename(columns={'Country_code': 'country_code', 'International_dialing': 'dialing_code'}, inplace=True)
customers_df = pd.merge(customers_df, dialing_df[['country_code', 'dialing_code']], on='country_code', how='left')

#phone numbers
customers_df['phone'] = customers_df['phone'].fillna(value=pd.Series([generate_p() for _ in range(len(customers_df))]))
customers_df['phone'] = customers_df['phone'].astype(str).str.replace(r'\D', '', regex=True)
 
#international code
customers_df['international_phone'] = customers_df['dialing_code'].fillna('')+'-'+ customers_df['phone']
 
#Customer Classification
tier_map = {'Gold':2, 'Silver':1,  'Bronze':0}
customers_df['Customer_Tier'] = customers_df['loyalty_status'].map(tier_map)
 
#Joining 
unified_df = pd.merge(orders_df, customers_df, on='customer_id', how='left')
 
#Loading
unified_df.to_sql('unified_customer_view3', con=engine, if_exists='replace', index=False)
 
 
