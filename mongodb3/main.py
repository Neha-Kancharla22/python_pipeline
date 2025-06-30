# main.py
from extract import  read_config,extract_documents
from transform import transform_data
from load2 import load_to_sql


def main():

   print("Starting ETL pipeline...")

    #Reading configuration
   config = read_config()
   print("Configuration loaded.")

    #Extracting data from MongoDB
   print("Extracting documents from MongoDB...")
   raw_df = extract_documents()

    # Transform the extracted data
   print("Transforming data...")
   transformed_df = transform_data()

    # Load transformed data to SQL Server
   print("Loading data into SQL Server...")
   load_to_sql()

   print(" ETL pipeline completed successfully.")

if __name__ == "__main__":
    main()
    


