import pandas as pd
import os
import re
from datetime import datetime
import logging
from sqlalchemy import text, create_engine, MetaData, Table
import dash
import dash_bootstrap_components as dbc

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Define paths
inventory_dir = r'C:\Users\TE582412\Desktop\Plant KPIS\Automotive\Inventory\SAP Inventory'
conversion_rate = 1.072  # Example conversion rate, update with the current rate

# Function to extract year and week number using regex
def extract_year_week(folder_name):
    logging.info(f"Extracting year and week number from folder name: {folder_name}")
    match = re.search(r'(\d{4})-WK(\d+)', folder_name)
    if match:
        year = int(match.group(1))
        week = int(match.group(2))
        logging.info(f"Extracted year: {year}, week: {week}")
        return year, week
    else:
        logging.error(f"Year and week number not found in folder name: {folder_name}")
        raise ValueError(f"Year and week number not found in folder name: {folder_name}")

# Function to extract date from filename and return the day name
def extract_day_from_filename(filename, week_number):
    logging.info(f"Extracting day from filename: {filename}")
    match = re.search(r'(\d{2})\.(\d{2})\.(\d{2})', filename)
    if match:
        day, month, year_suffix = match.groups()
        full_date = f"20{year_suffix}-{month}-{day}"  # Assuming the century is 2000s
        date_obj = datetime.strptime(full_date, '%Y-%m-%d')
        day_name = date_obj.strftime('%A')
        formatted_day_name = f"WK{week_number}-{day_name}"
        logging.info(f"Extracted date: {full_date}, day name: {formatted_day_name}")
        return formatted_day_name
    else:
        logging.error(f"Date not found in filename: {filename}")
        raise ValueError(f"Date not found in filename: {filename}")

# Function to process a single inventory file
def process_inventory_file(file_path, year, week_number, engine):
    logging.info(f"Processing inventory file: {file_path}")
    day_name = extract_day_from_filename(os.path.basename(file_path), week_number)
    
    chunk = pd.read_excel(file_path)
    
    # Select only the required columns
    columns_to_select = [
        'Plant', 'Material', 'Material Description', 'Total Current Value of Stock', 
        'Total Stock', 'Standard Price', 'Per', 'MRP ctrlr', 'MRP controller name'
    ]
    chunk = chunk[columns_to_select]
    
    logging.debug(f"Columns in the file {file_path}: {chunk.columns.tolist()}")
    
    chunk['Material-Plant'] = chunk['Material'].astype(str).str.strip() + chunk['Plant'].astype(str).str.zfill(4).str.strip()
    chunk['MRP ctrlr'] = chunk['MRP ctrlr'].astype(str).str.strip()
    chunk['Material'] = chunk['Material'].astype(str).str.strip()
    chunk['Plant'] = chunk['Plant'].astype(str).str.strip()
    chunk['Year'] = year
    chunk['Week'] = week_number
    chunk['Day'] = day_name
    
    # Convert Total Current Value of Stock from euros to dollars
    chunk['Total Current Value of Stock ($)'] = chunk['Total Current Value of Stock'] * conversion_rate
    
    logging.debug(f"First few rows after adding Year, Week, and Day columns:\n{chunk.head()}")
    
    with engine.connect() as connection:
        # Query the SQL tables
        team = pd.read_sql('SELECT * FROM Teams_Table', connection)
        segment = pd.read_sql('SELECT * FROM Segments_Table', connection)
        projects = pd.read_sql('SELECT * FROM Projects_Table', connection)
        
    inventory_team = pd.merge(chunk, team, on='MRP ctrlr', how='left')
    inventory_seg_team = pd.merge(inventory_team, segment, on='Material-Plant', how='left').drop_duplicates(subset=['Material-Plant'])
    inventory_final = pd.merge(inventory_seg_team, projects, left_on='Material', right_on='Material Number', how='left')
    
    return inventory_final


def remove_obsolete_data(engine):
    actual_folders = [f for f in os.listdir(inventory_dir) if os.path.isdir(os.path.join(inventory_dir, f))]
    existing_weeks = [extract_year_week(folder)[1] for folder in actual_folders]

    logging.info(f"Existing weeks in the directory: {existing_weeks}")

    if not existing_weeks:
        logging.info("No existing weeks found in the directory. Skipping deletion.")
        return

    weeks_str = ', '.join(map(str, existing_weeks))

    with engine.connect() as connection:
        transaction = connection.begin()  # Begin a transaction
        try:
            # Delete obsolete weeks in Inventory table
            delete_query_inventory = f"""
                DELETE FROM Inventory
                WHERE Week NOT IN ({weeks_str})
            """
            connection.execute(text(delete_query_inventory))
            logging.info(f"Obsolete data successfully removed from Inventory table for weeks not in {existing_weeks}.")

            # Delete obsolete weeks in PurchasedParts table
            delete_query_purchased_parts = f"""
                DELETE FROM PurchasedParts
                WHERE Week NOT IN ({weeks_str})
            """
            connection.execute(text(delete_query_purchased_parts))
            logging.info(f"Obsolete data successfully removed from PurchasedParts table for weeks not in {existing_weeks}.")

            transaction.commit()  # Commit the transaction to persist the changes
        except Exception as e:
            transaction.rollback()  # Rollback the transaction in case of an error
            logging.error(f"Error during deletion: {e}")

def run_etl(engine):
    metadata = MetaData()
    processed_files_log = 'processed_files.log'
    processed_folders_log = 'processed_folders.log'

    # Load existing processed files and folders logs
    if os.path.exists(processed_files_log):
        with open(processed_files_log, 'r') as f:
            processed_files = f.read().splitlines()
    else:
        processed_files = []

    if os.path.exists(processed_folders_log):
        with open(processed_folders_log, 'r') as f:
            processed_folders = f.read().splitlines()
    else:
        processed_folders = []

    # Get actual folders and files from the directory
    actual_folders = [f for f in os.listdir(inventory_dir) if os.path.isdir(os.path.join(inventory_dir, f))]
    actual_files = [os.path.join(root, file) for folder in actual_folders for root, _, files in os.walk(os.path.join(inventory_dir, folder)) for file in files]

    # Remove folders and files from logs if they no longer exist
    processed_folders = [folder for folder in processed_folders if folder in actual_folders]
    processed_files = [file for file in processed_files if os.path.basename(file) in [os.path.basename(actual_file) for actual_file in actual_files]]

    # Save the updated logs
    with open(processed_files_log, 'w') as f:
        f.write('\n'.join(processed_files))

    with open(processed_folders_log, 'w') as f:
        f.write('\n'.join(processed_folders))

    # Remove obsolete data from the database
    #remove_obsolete_data(engine)

    # Proceed with processing new data as before
    new_folders = [f for f in os.listdir(inventory_dir) if os.path.isdir(os.path.join(inventory_dir, f)) and f not in processed_folders]

    # Initialize an empty dataframe for the global combined data
    global_combined_dataframe = pd.DataFrame()
    new_data_processed = False

    try:
        for folder in new_folders:
            folder_path = os.path.join(inventory_dir, folder)
            year, week_number = extract_year_week(folder)
            
            new_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.XLSX') and os.path.basename(f) not in processed_files]
            
            for file in new_files:
                processed_df = process_inventory_file(file, year, week_number, engine)
                global_combined_dataframe = pd.concat([global_combined_dataframe, processed_df], ignore_index=True)
                processed_files.append(os.path.basename(file))
                new_data_processed = True
            
            processed_folders.append(folder)

        if new_data_processed:
            logging.info("Successfully processed and combined all new files into a global dataframe.")

            # Remove unnamed columns
            global_combined_dataframe = global_combined_dataframe.loc[:, ~global_combined_dataframe.columns.str.contains('^Unnamed')]
            # Remove duplicates
            global_combined_dataframe = global_combined_dataframe.drop_duplicates()

            logging.info("Processing complete.")
        else:
            logging.info("No new data found to process.")

    except ValueError as e:
        logging.error(f"Error during processing: {e}")

    if new_data_processed:
        # Filter the required columns
        columns_to_keep = [
            'Plant', 'Material', 'Material Description', 'Total Stock', 
            'Standard Price', 'Per', 'MRP ctrlr', 'MRP controller name', 
            'Year', 'Week', 'Day', 'Total Current Value of Stock ($)', 
            'Team', 'Segment', 'Project'
        ]
        global_combined_dataframe = global_combined_dataframe[columns_to_keep]

        # Merge with purchased parts data from the database
        with engine.connect() as connection:
            purchased_parts = pd.read_sql('SELECT * FROM Purchased_parts_reduced_db', connection)
            
        inventory_purchased_parts_combined = (
            pd.merge(global_combined_dataframe, purchased_parts, on=['Material', 'Team'], how='inner')
            .dropna(subset=['vendor Name / plant', 'Local/europe/Overseas'], how='all')
            .query("Segment == 'purchased component'")
        )

        # Select columns for PurchasedParts table
        columns_purchased_parts = [
            'Material','Plant', 'vendor Name / plant', 'Day', 'Week', 'Year', 'Total Current Value of Stock ($)'
        ]
        inventory_purchased_parts_combined = inventory_purchased_parts_combined[columns_purchased_parts]

        # Connect to SQL Server and load data using SQLAlchemy
        try:
            logging.info("Connecting to SQL Server and loading data.")
            with engine.connect() as connection:
                # Create Inventory table if not exists
                create_inventory_table_query = """
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Inventory' AND xtype='U')
                CREATE TABLE Inventory (
                    Plant NVARCHAR(255),
                    Material NVARCHAR(255),
                    [Material Description] NVARCHAR(255),
                    [Total Stock] FLOAT,
                    [Standard Price] FLOAT,
                    Per NVARCHAR(255),
                    [MRP ctrlr] NVARCHAR(255),
                    [MRP controller name] NVARCHAR(255),
                    Year INT,
                    Week INT,
                    Day NVARCHAR(255),
                    [Total Current Value of Stock ($)] FLOAT,
                    Team NVARCHAR(255),
                    Segment NVARCHAR(255),
                    Project NVARCHAR(255)
                )
                """
                connection.execute(text(create_inventory_table_query))

                # Create PurchasedParts table if not exists
                create_purchased_parts_table_query = """
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='PurchasedParts' AND xtype='U')
                CREATE TABLE PurchasedParts (
                    Material NVARCHAR(255),
                    [vendor Name / plant] NVARCHAR(255),
                    Day NVARCHAR(255),
                    Week INT,
                    Year INT,
                    [Total Current Value of Stock ($)] FLOAT
                )
                """
                connection.execute(text(create_purchased_parts_table_query))

            # Load the data into the SQL tables
            global_combined_dataframe.to_sql('Inventory', engine, if_exists='append', index=False)
            inventory_purchased_parts_combined.to_sql('PurchasedParts', engine, if_exists='append', index=False)
            logging.info("Data successfully loaded into SQL Server.")
        except Exception as e:
            logging.error(f"Error loading data into SQL Server: {e}")

        # Update the log of processed files and folders
        with open(processed_files_log, 'w') as f:
            f.write('\n'.join(processed_files))

        with open(processed_folders_log, 'w') as f:
            f.write('\n'.join(processed_folders))
    
    return new_data_processed
