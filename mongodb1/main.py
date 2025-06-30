from extract import extract_documents, connect_sql_server, read_config
from transform import transform_documents
from load import load_to_sql

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
    transformed_df = transform_documents()

    # Load transformed data to SQL Server
    print("Loading data into SQL Server...")
    load_to_sql()

    print(" ETL pipeline completed successfully.")

if __name__ == "__main__":
    main()
    