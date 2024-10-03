import pandas as pd
import os
import re
from datetime import datetime
import logging
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, text
from sqlalchemy.exc import SQLAlchemyError
import pyodbc
import numpy as np

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define paths
ZBMT_dir = r'C:\Users\TE582412\Desktop\Plant KPIS\Automotive\EDI\ZBMT data for test'
segment_path = r'C:\Users\TE582412\Desktop\Plant KPIS\Automotive\Csv Data\CSV_reduced data base original\CSV_reduced data base original_segment.csv'
team_path = r'C:\Users\TE582412\Desktop\Plant KPIS\Automotive\Csv Data\CSV_reduced data base original\CSV_reduced data base original_team.csv'
projects_path = r'C:\Users\TE582412\Desktop\Plant KPIS\Automotive\Csv Data\CSV_Projects\CSV_Projects data_Sheet1.csv'
config_path = r'C:\Users\TE582412\Desktop\Plant KPIS\Automotive\Csv Data\CSV_Config\CSV_config_Sheet1.csv'

# Read the CSV files
segment = pd.read_csv(segment_path)
team = pd.read_csv(team_path)
Projects = pd.read_csv(projects_path)
rows_to_remove = pd.read_csv(config_path)

# Ensure the 'Shipping Plant' column in rows_to_remove is of type str
rows_to_remove['Shipping Plant'] = rows_to_remove['Shipping Plant'].astype(str)

# Function to extract year-week and week number from the file name
def extract_year_week(file_name):
    date_str = re.search(r'(\d{2}\.\d{2}\.\d{4})', file_name).group(1)
    date_obj = datetime.strptime(date_str, '%d.%m.%Y')
    year_week = f"{date_obj.year}-W{date_obj.isocalendar()[1]}"
    week_number = date_obj.isocalendar()[1]
    return year_week, date_obj.year, week_number

# Function to process a single chunk of data
def process_chunk(file_path, year_week, year, week_number):
    df_chunk = pd.read_excel(file_path)
    
    # Remove leading and trailing spaces from column names
    df_chunk.columns = df_chunk.columns.str.strip()
    
    # Ensure Shipping Plant has 4 characters by adding leading zero if necessary
    df_chunk['Shipping Plant'] = df_chunk['Shipping Plant'].apply(lambda x: f"{int(x):04d}" if pd.notnull(x) else x)
    df_chunk['Shipping Plant'] = df_chunk['Shipping Plant'].astype(str)

    # Create Material-Plant column
    df_chunk['Material-Plant'] = df_chunk['Material Nbr'].astype(str) + df_chunk['Shipping Plant']

    # Add Year, Year-Week, and Week Number columns
    df_chunk['Year'] = year
    df_chunk['Year-Week'] = year_week
    df_chunk['Week Number'] = week_number
    
    # Add Shipment Month column
    df_chunk['Cust Req S'] = pd.to_datetime(df_chunk['Cust Req S'], errors='coerce', dayfirst=True)
    df_chunk['Shipment Month'] = df_chunk['Cust Req S'].dt.strftime('%Y-%B')

    # Clean the data by removing specified rows
    df_chunk = pd.merge(df_chunk, rows_to_remove, on=['SoldTo', 'Shipping Plant', 'Soldto Name'], how='left', indicator=True)
    df_chunk = df_chunk[df_chunk['_merge'] == 'left_only'].drop(columns=['_merge'])

    # Join with team and segment data
    df_chunk = df_chunk.merge(team, left_on='MRP Ctl', right_on='MRP ctrlr', how='left')
    df_chunk = df_chunk.merge(segment, left_on='Material-Plant', right_on='Material-Plant', how='left')
    df_chunk = df_chunk.merge(Projects, left_on='Material Nbr', right_on='Material Number', how='left')

    # Drop redundant columns after merge
    df_chunk = df_chunk.drop(columns=['Material Number', 'MRP ctrlr'], errors='ignore')

    # Drop duplicates after join
    df_chunk = df_chunk.drop_duplicates()

    # Ensure 'Bklg Value' is numeric
    if 'Bklg Value' in df_chunk.columns:
        # Remove commas and spaces, then convert to float
        df_chunk['Bklg Value'] = df_chunk['Bklg Value'].str.replace(',', '').str.replace(' ', '').astype(float)
    else:
        logging.warning("Column 'Bklg Value' is missing in the input file.")
        df_chunk['Bklg Value'] = None

    return df_chunk

# Process all inventory files in the directories
logging.info("Processing all ZBMT files in the directories.")
all_files = [os.path.join(ZBMT_dir, f) for f in os.listdir(ZBMT_dir) if f.endswith('.xlsx')]
logging.info(f"Total files to process: {len(all_files)}")

if not all_files:
    logging.warning("No files found to process. Please check the directories and file extensions.")

# Initialize an empty dataframe for the global combined data
global_combined_dataframe = pd.DataFrame()

try:
    for file in all_files:
        file_name = os.path.basename(file)
        year_week, year, week_number = extract_year_week(file_name)
        logging.info(f"Processing file {file} for week {year_week} of year {year}")
        processed_df = process_chunk(file, year_week, year, week_number)
        global_combined_dataframe = pd.concat([global_combined_dataframe, processed_df], ignore_index=True)

    logging.info("Successfully processed and combined all files into a global dataframe.")

    # Remove leading and trailing spaces from column names of the global dataframe
    global_combined_dataframe.columns = global_combined_dataframe.columns.str.strip()

    # Remove unnamed columns
    global_combined_dataframe = global_combined_dataframe.loc[:, ~global_combined_dataframe.columns.str.contains('^Unnamed')]
    # Remove duplicates
    global_combined_dataframe = global_combined_dataframe.drop_duplicates()

    # Display number of rows and first 10 rows
    num_rows = len(global_combined_dataframe)
    logging.info(f"Total number of rows in the combined dataframe: {num_rows}")
    logging.info(f"First 10 rows of the combined dataframe:\n{global_combined_dataframe.head(10)}")

    logging.info("Processing complete.")

    # Filter the global_combined_dataframe to keep only the specified columns
    columns_to_keep = [
        'Document', 'Material Nbr', 'Shipping Plant', 'MRP Ctl', 'MRP Name', 'Quantity', 'Bklg Value', 'Curr', 'Cust Req S', 'SoldTo', 'Soldto Name', 'Material Description',
        'UnrestUseDelvQty', 'Material-Plant', 'Year', 'Year-Week', 'Week Number', 'Shipment Month', 'Team', 'Segment', 'Project'
    ]
    global_combined_dataframe = global_combined_dataframe[columns_to_keep]

    # Define the database connection details
    conn_str = (
        'mssql+pyodbc://MAN61NBO1VZ06Y2\\SQLEXPRESS/Plantkpis_Sp_db?'
        'driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
    )

    # Create the engine with fast_executemany enabled
    engine = create_engine(conn_str, fast_executemany=True)
    metadata = MetaData()

    # Define the table schema
    edi_sap_table = Table(
        'EDI_SAP_Table', metadata,
        Column('Document', String),
        Column('Material_Nbr', String),
        Column('Shipping_Plant', String),
        Column('MRP_Ctl', String),
        Column('MRP_Name', String),
        Column('Quantity', Float),  # Changed to Float
        Column('Bklg_Value', Float),
        Column('Curr', String),
        Column('Cust_Req_S', String),
        Column('SoldTo', String),
        Column('Soldto_Name', String),
        Column('Material_Description', String),
        Column('UnrestUseDelvQty', Float),
        Column('Material_Plant', String),
        Column('Year', Integer),
        Column('Year_Week', String),
        Column('Week_Number', Integer),
        Column('Shipment_Month', String),
        Column('Team', String),
        Column('Segment', String),
        Column('Project', String)
    )

    # Create the table if it doesn't exist
    try:
        with engine.connect() as connection:
            logging.info("Connecting to SQL Server and creating table if not exists.")
            connection.execute(text("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='EDI_SAP_Table' AND xtype='U')
            BEGIN
                CREATE TABLE EDI_SAP_Table (
                    Document NVARCHAR(MAX),
                    Material_Nbr NVARCHAR(MAX),
                    Shipping_Plant NVARCHAR(MAX),
                    MRP_Ctl NVARCHAR(MAX),
                    MRP_Name NVARCHAR(MAX),
                    Quantity FLOAT,
                    Bklg_Value FLOAT,
                    Curr NVARCHAR(MAX),
                    Cust_Req_S NVARCHAR(MAX),
                    SoldTo NVARCHAR(MAX),
                    Soldto_Name NVARCHAR(MAX),
                    Material_Description NVARCHAR(MAX),
                    UnrestUseDelvQty FLOAT,
                    Material_Plant NVARCHAR(MAX),
                    Year INT,
                    Year_Week NVARCHAR(MAX),
                    Week_Number INT,
                    Shipment_Month NVARCHAR(MAX),
                    Team NVARCHAR(MAX),
                    Segment NVARCHAR(MAX),
                    Project NVARCHAR(MAX)
                );
            END
            """))
            logging.info("Table creation check complete.")
            logging.info("Starting data insertion into EDI_SAP_Table.")
            
            chunksize = 1000  # Set the chunk size to 5000 rows
            for i, chunk in enumerate(np.array_split(global_combined_dataframe, len(global_combined_dataframe) // chunksize)):
                logging.info(f"Inserting chunk {i + 1}")
                chunk.to_sql('EDI_SAP_Table', con=engine, if_exists='append', index=False, method='multi')
                logging.info(f"Inserted chunk {i + 1} successfully")
                
            logging.info("Data successfully inserted into EDI_SAP_Table.")
    except SQLAlchemyError as e:
        logging.error(f"Error inserting data into EDI_SAP_Table: {e}")

except Exception as e:
    logging.error(f"Error during processing: {e}")

