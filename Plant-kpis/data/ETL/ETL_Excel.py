import pandas as pd
import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import re
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define file paths
file_path = r'C:\Users\TE582412\Desktop\Plant KPIS\Automotive\EDI\ZBMT data for test\New Microsoft Excel Worksheet 06.06.2024.xlsx'
segment_path = r'C:\Users\TE582412\Desktop\Plant KPIS\Automotive\Csv Data\CSV_reduced data base original\CSV_reduced data base original_segment.csv'
team_path = r'C:\Users\TE582412\Desktop\Plant KPIS\Automotive\Csv Data\CSV_reduced data base original\CSV_reduced data base original_team.csv'
projects_path = r'C:\Users\TE582412\Desktop\Plant KPIS\Automotive\Csv Data\CSV_Projects\CSV_Projects data_Sheet1.csv'
config_path = r'C:\Users\TE582412\Desktop\Plant KPIS\Automotive\Csv Data\CSV_Config\CSV_config_Sheet1.csv'

# Database connection string
engine = create_engine('mssql+pyodbc://MAN61NBO1VZ06Y2\\SQLEXPRESS/Plantkpis_Sp_db?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes', fast_executemany=True)

# Read the CSV files
segment = pd.read_csv(segment_path)
team = pd.read_csv(team_path)
Projects = pd.read_csv(projects_path)
rows_to_remove = pd.read_csv(config_path)

# Ensure the 'Shipping Plant' column in rows_to_remove is of type str
rows_to_remove['Shipping Plant'] = rows_to_remove['Shipping Plant'].astype(str)

# Function to extract Year, Year-Week, and Week Number from file name
def extract_year_week(filename):
    # Extract date string from filename using regex (assuming date is in DD.MM.YYYY format)
    date_str = re.search(r'(\d{2}\.\d{2}\.\d{4})', filename)
    if date_str:
        date_str = date_str.group(1)
        date_obj = datetime.strptime(date_str, '%d.%m.%Y')
        year = date_obj.year
        week_number = date_obj.isocalendar()[1]
        year_week = f"{year}-W{week_number}"
        return year, year_week, week_number
    else:
        logging.error("Filename does not contain a valid date format.")
        return None, None, None

# Function to process the Excel file
def process_file(df_chunk, filename=None):
    # Assuming df_chunk is already loaded from the uploaded file
    
    # Remove leading and trailing spaces from column names
    df_chunk.columns = df_chunk.columns.str.strip()
    
    # Extract Year, Year-Week, and Week Number from the filename or data
    if filename:
        year, year_week, week_number = extract_year_week(filename)
        df_chunk['Year'] = year
        df_chunk['Year-Week'] = year_week
        df_chunk['Week Number'] = week_number
    else:
        # Handle the case where filename is not provided or extraction fails
        df_chunk['Year'] = None
        df_chunk['Year-Week'] = None
        df_chunk['Week Number'] = None
    
    # Ensure Shipping Plant has 4 characters by adding leading zero if necessary
    df_chunk['Shipping Plant'] = df_chunk['Shipping Plant'].apply(lambda x: f"{int(x):04d}" if pd.notnull(x) else x)
    df_chunk['Shipping Plant'] = df_chunk['Shipping Plant'].astype(str)

    # Create Material-Plant column
    df_chunk['Material-Plant'] = df_chunk['Material Nbr'].astype(str) + df_chunk['Shipping Plant']

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

    # Ensure 'Document' is treated as a string
    df_chunk['Document'] = df_chunk['Document'].astype(str)

    # Ensure 'Bklg Value' is numeric
    if 'Bklg Value' in df_chunk.columns:
        # Remove commas and spaces, then convert to float
        df_chunk['Bklg Value'] = df_chunk['Bklg Value'].str.replace(',', '').str.replace(' ', '').astype(float)
    else:
        logging.warning("Column 'Bklg Value' is missing in the input file.")
        df_chunk['Bklg Value'] = None

    # Ensure 'Quantity' is numeric
    df_chunk['Quantity'] = df_chunk['Quantity'].str.replace(',', '').str.replace(' ', '').astype(float)

    # Ensure 'UnrestUseDelvQty' is numeric
    df_chunk['UnrestUseDelvQty'] = df_chunk['UnrestUseDelvQty'].str.replace(',', '').str.replace(' ', '').astype(float)

    # Fill any NaN values in numeric columns with 0
    df_chunk['Quantity'] = df_chunk['Quantity'].fillna(0)
    df_chunk['UnrestUseDelvQty'] = df_chunk['UnrestUseDelvQty'].fillna(0)
    df_chunk['Bklg Value'] = df_chunk['Bklg Value'].fillna(0)

    return df_chunk


# Function to insert processed data into the database
def insert_data_to_db(df_chunk):
    try:
        # Insert the data into the EDI_SAP_Table
        df_chunk.to_sql('EDI_SAP_Table', con=engine, if_exists='append', index=False)
        logging.info(f'Successfully inserted {len(df_chunk)} rows into the database.')
    except SQLAlchemyError as e:
        logging.error(f'Error inserting data into the database: {e}')

# Process the file and insert into the database
logging.info(f"Processing file {file_path}")
try:
    processed_df = process_file(file_path)
    logging.info("Successfully processed the file.")

    # Remove leading and trailing spaces from column names
    processed_df.columns = processed_df.columns.str.strip()

    # Remove unnamed columns
    processed_df = processed_df.loc[:, ~processed_df.columns.str.contains('^Unnamed')]
    
    # Remove duplicates
    processed_df = processed_df.drop_duplicates()

    # Filter the dataframe to keep only the specified columns
    columns_to_keep = [
        'Document', 'Material Nbr', 'Shipping Plant', 'MRP Ctl', 'MRP Name', 'Quantity', 'Bklg Value', 'Curr', 'Cust Req S', 'SoldTo', 'Soldto Name', 'Material Description',
        'UnrestUseDelvQty', 'Material-Plant', 'Shipment Month', 'Team', 'Segment', 'Project', 'Year', 'Year-Week', 'Week Number'
    ]
    processed_df = processed_df[columns_to_keep]

    # Insert the processed dataframe into the database
    #insert_data_to_db(processed_df)

except Exception as e:
    logging.error(f"Error during processing: {e}")
