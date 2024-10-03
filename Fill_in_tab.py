import pandas as pd
from sqlalchemy import create_engine

# Database connection string
conn_str = (
    'mssql+pyodbc://MAN61NBO1VZ06Y2\SQLEXPRESS/Plantkpis_Sp_db?'
    'driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
)
purchased_parts_path = r'C:\Users\TE582412\Desktop\Plant KPIS\Automotive\Csv Data\CSV_purchased parts reduced data\CSV_purchased parts reduced data_to_use.csv'

# Create an engine
engine = create_engine(conn_str)

purchased_parts = pd.read_csv(purchased_parts_path)

# Load DataFrames into SQL tables

purchased_parts.to_sql('Purchased_parts_reduced_db', engine, if_exists='replace', index=False)
