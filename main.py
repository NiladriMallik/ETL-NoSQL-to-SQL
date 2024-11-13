from google.cloud import storage
import json
import pandas as pd
from sqlalchemy import create_engine
import getpass

# Initialize the GCP storage client
def initialize_storage_client(service_account_key_path):
    return storage.Client.from_service_account_json(service_account_key_path)


# Download files from the GCP bucket
def download_nosql_files(bucket_name, prefix, service_account_key_path):
    client = initialize_storage_client(service_account_key_path)
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix = prefix)

    nosql_data = []

    for blob in blobs:
        file_data = blob.download_as_text()
        json_data = json.loads(file_data)
        nosql_data.append(json_data) # Append each JSON document to a list

    return nosql_data

# Convert JSON document to DataFrames
def convert_json_to_dataframes(json_data):
    return pd.json_normalize(json_data) # Flatten nested structures if needed


# Function to write dataframe to SQL
def write_to_sql(dataframe, table_name, engine):
    dataframe.to_sql(table_name, con = engine, if_exists = 'replace', index = False)

# Automating the entire process
def nosql_to_sql_pipeline(bucket_name, prefix, service_account_key_path, database_uri):
    # Download NoSQL documents from GCP bucket
    nosql_data = download_nosql_files(bucket_name, prefix, service_account_key_path)

    # Convert JSON data to DataFrames
    dataframes = [convert_json_to_dataframes(doc) for doc in nosql_data]

    # Initialize database engine
    engine = create_engine(database_uri)

    # Write each dataframe to a separate SQL table
    for idx, df in enumerate(dataframes):
        table_name = f'test_table_{idx}'
        write_to_sql(df, table_name, engine)

#############################################################################################

# Configure SQLAlchemy engine (to be replaced with our database URI)
username = input('Enter username: ')
password = getpass.getpass('Enter password: ')

# Reading MySQL DB details from JSON file
with open('MySQL_connection_params.json') as db_connection_file:
    db_connection_data = json.load(db_connection_file)

mysql_host = db_connection_data["host"]
mysql_port = db_connection_data["port"]
mysql_database = db_connection_data['database']

# Reading bucket connection details
with open('Bucket_connection_parameters.json') as bucket_connection_file:
    bucket_connection_data = json.load(bucket_connection_file)

bucket_name = bucket_connection_data["bucket_name"]
prefix = bucket_connection_data["prefix"]

bucket_service_key = db_connection_data['service_key']

database_uri = f'mysql+pymysql://{username}:{password}@{mysql_host}/{mysql_database}'
engine = create_engine(database_uri)

nosql_to_sql_pipeline(bucket_name, prefix, bucket_service_key, database_uri)