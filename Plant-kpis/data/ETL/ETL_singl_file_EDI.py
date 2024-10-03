import pandas as pd
import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import re
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
import numpy as np

def reduce_mem_usage(df):
    """Iterate through all the columns of a dataframe and modify the data type
       to reduce memory usage.
    """
    start_mem = df.memory_usage().sum() / 1024**2
    for col in df.columns:
        col_type = df[col].dtype
        if col_type != object:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
        else:
            df[col] = df[col].astype('category')
    end_mem = df.memory_usage().sum() / 1024**2
    return df

# Define file paths
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

# Improved function to extract Year, Year-Week, and Week Number from the filename
def extract_year_week_day(file_name):
    try:
        logging.info(f"Attempting to extract date from filename: {file_name}")
        # Extract date from the filename using regex
        date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', file_name)
        if date_match:
            date_str = date_match.group(1)
            logging.info(f"Extracted date string: {date_str}")
            # Convert the date string to a datetime object
            date_obj = datetime.strptime(date_str, '%d.%m.%Y')
            # Extract year and week number
            year = date_obj.year
            week_number = date_obj.isocalendar()[1]
            day_name = date_obj.strftime('%A')  # Get the day name
            # Create a Year-Week string
            year_week = f"{year}-W{week_number}"
            # Create WKX-Day format
            day = f"WK{week_number}-{day_name}"
            # Return the extracted year, year-week, week number, and day
            return year, year_week, week_number, day
        else:
            logging.error(f"Date pattern not found in the filename: {file_name}")
            return None, None, None, None
    except Exception as e:
        logging.error(f"Failed to extract date from filename '{file_name}': {e}")
        return None, None, None, None

def process_file(df_chunk, file_name=None):
    logging.info("Starting to process the DataFrame.")
    
    # Remove leading and trailing spaces from column names
    df_chunk.columns = df_chunk.columns.str.strip()
    logging.info(f"Column names after stripping spaces: {df_chunk.columns.tolist()}")

    # Initialize Year, Year-Week, and Week Number columns with None
    df_chunk['Year'] = None
    df_chunk['Year-Week'] = None
    df_chunk['Week Number'] = None
    df_chunk['Day'] = None

    # Log the filename
    logging.info(f"Processing file: {file_name}")

    # Extract Year, Year-Week, and Week Number from the filename
    if file_name:
        year, year_week, week_number, day = extract_year_week_day(file_name)
        
        # Log the extracted values
        logging.info(f"Extracted Year={year}, Year-Week={year_week}, Week Number={week_number}, Day={day}")
        
        # Assign the extracted values to the DataFrame
        df_chunk['Year'] = year
        df_chunk['Year-Week'] = year_week
        df_chunk['Week Number'] = week_number
        df_chunk['Day'] = day

    # Check if the columns exist before logging
    if 'Year' in df_chunk.columns and 'Year-Week' in df_chunk.columns and 'Week Number' in df_chunk.columns and 'Day' in df_chunk.columns:
        logging.info(f"DataFrame after adding Year, Year-Week, Week Number, and Day:\n{df_chunk[['Year', 'Year-Week', 'Week Number', 'Day']].head()}")
    else:
        logging.warning("Year, Year-Week, Week Number, or Day columns are missing from the DataFrame.")
    # Ensure Shipping Plant has 4 characters by adding leading zero if necessary
    df_chunk['Shipping Plant'] = df_chunk['Shipping Plant'].apply(lambda x: f"{int(x):04d}" if pd.notnull(x) else x)
    df_chunk['Shipping Plant'] = df_chunk['Shipping Plant'].astype(str)
    logging.info("Processed 'Shipping Plant' column.")

    # Create Material-Plant column
    df_chunk['Material-Plant'] = df_chunk['Material Nbr'].astype(str) + df_chunk['Shipping Plant']

    # Add Shipment Month column
    df_chunk['Cust Req S'] = pd.to_datetime(df_chunk['Cust Req S'], errors='coerce', dayfirst=True)
    df_chunk['Shipment Month'] = df_chunk['Cust Req S'].dt.strftime('%Y-%B')
    logging.info("Added 'Shipment Month' column.")

    # Clean the data by removing specified rows
    df_chunk = pd.merge(df_chunk, rows_to_remove, on=['SoldTo', 'Shipping Plant', 'Soldto Name'], how='left', indicator=True)
    df_chunk = df_chunk[df_chunk['_merge'] == 'left_only'].drop(columns=['_merge'])
    logging.info("Cleaned data by removing specified rows.")

    # Join with team and segment data
    df_chunk = df_chunk.merge(team, left_on='MRP Ctl', right_on='MRP ctrlr', how='left')
    df_chunk = df_chunk.merge(segment, left_on='Material-Plant', right_on='Material-Plant', how='left')
    df_chunk = df_chunk.merge(Projects, left_on='Material Nbr', right_on='Material Number', how='left')
    logging.info("Joined with team and segment data.")

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

    # Remove unnamed columns
    df_chunk = df_chunk.loc[:, ~df_chunk.columns.str.contains('^Unnamed')]
    
    # Remove duplicates
    df_chunk = df_chunk.drop_duplicates()

    # Filter the dataframe to keep only the specified columns
    columns_to_keep = [
        'Document', 'Material Nbr', 'Shipping Plant', 'MRP Ctl', 'MRP Name', 'Quantity', 'Bklg Value', 'Curr', 'Cust Req S', 'SoldTo', 'Soldto Name', 'Material Description',
        'UnrestUseDelvQty', 'Material-Plant', 'Shipment Month', 'Team', 'Segment', 'Project', 'Year', 'Year-Week', 'Week Number','Day'
    ]
    
    # Ensure columns exist before selecting them
    df_chunk = df_chunk[[col for col in columns_to_keep if col in df_chunk.columns]]

    logging.info(f"Final DataFrame columns: {df_chunk.columns.tolist()}")
    logging.info(f"Final DataFrame preview:\n{df_chunk.head()}")

    return df_chunk

# Function to insert processed data into the database
def insert_data_to_db(df_chunk):
    try:
        # Insert the data into the EDI_SAP_Table_Copy
        df_chunk.to_sql('EDI_SAP_Table', con=engine, if_exists='append', index=False)
        logging.info(f'Successfully inserted {len(df_chunk)} rows into the database.')
    except SQLAlchemyError as e:
        logging.error(f'Error inserting data into the database: {e}')
