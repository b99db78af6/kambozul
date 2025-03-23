import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# db parameters
user = db_config.user
pwd = db_config.pwd
db_host = db_config.db_host
db_port = db_config.db_port
db_name = db_config.db_name
table_name = 'your_table'

# Create a connection string
connection_string = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'

# Create a SQLAlchemy engine
engine = create_engine(connection_string)

# Define the directory containing the CSV files
directory_path = 'path/to/your/directory'

# Iterate over each file in the directory
for filename in os.listdir(directory_path):
    if filename.endswith('.csv'):  # Check if the file is a CSV
        csv_file_path = os.path.join(directory_path, filename)
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file_path)