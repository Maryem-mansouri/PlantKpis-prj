from sqlalchemy import create_engine, MetaData, Table, select
import pandas as pd
import logging
from data.ETL import Inventory_db_ETL

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

conn_str = (
    'mssql+pyodbc://MAN61NBO1VZ06Y2\SQLEXPRESS/Plantkpis_Sp_db?'
    'driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
)

engine = create_engine(conn_str)
metadata = MetaData()

data_store = {}

# Function to refresh the data store
def refresh_data_store():
    global data_store
    data_store = {}
    logging.info("Data store has been refreshed.")

# Ensure data freshness by running ETL and refreshing data store
def ensure_data_freshness():
    new_data_processed = Inventory_db_ETL.run_etl(engine)  # Pass the engine to the ETL process
    if new_data_processed:
        logging.info("New data was found and processed during the ETL run.")
        refresh_data_store()  # Clear and refresh data store after processing new data
        fetch_initial_data()  # Fetch the latest data
    else:
        logging.info("No new data was found during the ETL run.")

def fetch_initial_data():
    inventory_table = Table('Inventory', metadata, autoload_with=engine)
    purchased_parts_table = Table('PurchasedParts', metadata, autoload_with=engine)
    
    with engine.connect() as connection:
        inventory_df = pd.read_sql_table('Inventory', connection)
        purchased_parts_df = pd.read_sql_table('PurchasedParts', connection)
    
    data_store['inventory'] = inventory_df
    data_store['purchased_parts'] = purchased_parts_df
    logging.debug(f"Initial data fetched: {inventory_df.head()}, {purchased_parts_df.head()}")

# Fetch initial data when the module is loaded
ensure_data_freshness()
# Fetch initial data
fetch_initial_data()


