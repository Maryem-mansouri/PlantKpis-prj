import pandas as pd
import os
import re
from datetime import datetime
import logging
from sqlalchemy import create_engine, select, func, Table, MetaData
from sqlalchemy.sql import text
# Database connection string (Update if necessary)
engine = create_engine('mssql+pyodbc://MAN61NBO1VZ06Y2\\SQLEXPRESS/Plantkpis_Sp_db?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes', fast_executemany=True)
metadata = MetaData()

# Define the EDI_SAP_Table
edi_sap_table = Table('EDI_SAP_Table', metadata, autoload_with=engine)

def fetch_filter_options():
    with engine.connect() as connection:
        filter_plant = [row[0] for row in connection.execute(select(edi_sap_table.c['Shipping Plant']).distinct())]
        filter_year = [row[0] for row in connection.execute(select(edi_sap_table.c['Year']).distinct())]
        filter_week = [row[0] for row in connection.execute(select(edi_sap_table.c['Week Number']).distinct())]
        filter_ship_month = [row[0] for row in connection.execute(select(edi_sap_table.c['Shipment Month']).distinct())]
        filter_ship_date = [row[0] for row in connection.execute(select(edi_sap_table.c['Cust Req S']).distinct())]

    return filter_plant, filter_year, filter_week, filter_ship_month, filter_ship_date

# Fetch filter options from the database
filter_plant, filter_year, filter_week, filter_ship_month, filter_ship_date = fetch_filter_options()

def fetch_data(plant, year, week, ship_date, ship_month):
    logging.info(f"Filters - Plant: {plant}, Year: {year}, Week: {week}, Ship Date: {ship_date}, Ship Month: {ship_month}")
    
    with engine.connect() as connection:
        query = select(edi_sap_table).where(
    edi_sap_table.c['Shipping Plant'].in_(plant),
    edi_sap_table.c['Year'].in_(year),
    edi_sap_table.c['Week Number'].in_(week),
    edi_sap_table.c['Cust Req S'].in_(ship_date),
    edi_sap_table.c['Shipment Month'].in_([str(month) for month in ship_month])
)

        
        result = connection.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        logging.info(f"Fetched data: {df.head()}")
        return df
    
def get_next_six_months_from_filter(filter_ship_month):
    current_date = datetime.now()
    current_month = current_date.month
    current_year = current_date.year

    # Convert the string options to datetime objects for comparison
    valid_months = []
    for month_str in filter_ship_month:
        # Assuming month_str is in the format 'YYYY-Month'
        try:
            month_datetime = datetime.strptime(month_str, '%Y-%B')
            # If the month-year is within the next six months, include it
            if (month_datetime.year == current_year and month_datetime.month >= current_month) or \
               (month_datetime.year == current_year + 1 and month_datetime.month < current_month):
                valid_months.append(month_str)
        except ValueError:
            continue

    # Sort and limit to the next six months
    valid_months.sort(key=lambda date: datetime.strptime(date, '%Y-%B'))
    return valid_months[:3]

def create_summary_dataframe(df, group_by_cols, agg_cols):
    if isinstance(agg_cols, str):
        agg_cols = {agg_cols: 'sum'}
    elif not isinstance(agg_cols, dict):
        raise ValueError("agg_cols should be a dictionary with columns and their respective aggregation functions.")
    df = df.sort_values(by=group_by_cols)
    summary_df = df.groupby(group_by_cols).agg(agg_cols).reset_index()
    
    # Log summary dataframe
    logging.info(f"Summary DataFrame:\n{summary_df.head()}")
    
    return summary_df

def create_pivot_table(df, index_col, columns_col, values_col):
    df[columns_col] = df[columns_col].astype(int)
    df = df.sort_values(by=columns_col)

    pivot_table = df.pivot_table(
        values=values_col,
        index=index_col,
        columns=columns_col,
        aggfunc='sum',
        fill_value=0
    )
    
    totals = pivot_table.sum(axis=0).to_frame().T
    totals[index_col] = 'Total'
    totals = totals.set_index(index_col)
    
    pivot_table = pd.concat([pivot_table, totals])
    
    weeks = sorted(df[columns_col].unique())
    if len(weeks) > 1:
        last_week = weeks[-1]
        previous_week = weeks[-2]
        pivot_table['Gap'] = pivot_table[last_week] - pivot_table[previous_week]
        pivot_table['Gap %'] = ((pivot_table['Gap'] / pivot_table[previous_week]) * 100).fillna(0).round(1).astype(str) + '%'
    else:
        pivot_table['Gap'] = 0
        pivot_table['Gap %'] = 0
    
    pivot_table = pivot_table.reset_index()
    
    # Log pivot table
    logging.info(f"Pivot Table:\n{pivot_table.head()}")
    
    return pivot_table

def create_pivot_table_with_columns(df, columns, values):
    # Ensure the values are numerical
    df[values] = pd.to_numeric(df[values], errors='coerce')
    
    pivot_table = df.pivot_table(
        values=values,
        columns=columns,
        aggfunc='sum',
        fill_value=0
    ).reset_index()

    weeks = [col for col in pivot_table.columns if col != 'index']
    if len(weeks) > 1:
        last_week = weeks[-1]
        previous_week = weeks[-2]
        pivot_table['Gap'] = pivot_table[last_week] - pivot_table[previous_week]
        pivot_table['Gap %'] = ((pivot_table['Gap'] / previous_week) * 100).fillna(0).round(1).astype(str) + '%'
    else:
        pivot_table['Gap'] = 0
        pivot_table['Gap %'] = 0
    
    # Log pivot table with columns
    logging.info(f"Pivot Table with Columns:\n{pivot_table.head()}")

    return pivot_table

def create_customer_demand_pivot(df, index_col, columns_col, values_col, document_col, material_nbr_col, cust_req_s_col):
    # Ensure that the required columns are present
    required_columns = [document_col, material_nbr_col, cust_req_s_col]
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"The DataFrame must contain the following columns: {', '.join(required_columns)}")
    
    # Convert the columns_col to int for sorting, if necessary
    df[columns_col] = df[columns_col].astype(int)
    df = df.sort_values(by=columns_col)

    # Create the pivot table
    pivot_table = df.pivot_table(
        values=values_col,
        index=[index_col, document_col, material_nbr_col, cust_req_s_col],  # Adding the passed columns
        columns=columns_col,
        aggfunc='sum',
        fill_value=0
    )
    
    # Calculate total demand per week and add a totals row
    totals = pivot_table.sum(axis=0).to_frame().T
    totals[index_col] = 'Total'
    totals[document_col] = ''  # Leave empty or set a specific value
    totals[material_nbr_col] = ''  # Leave empty or set a specific value
    totals[cust_req_s_col] = ''  # Leave empty or set a specific value
    totals = totals.set_index([index_col, document_col, material_nbr_col, cust_req_s_col])
    pivot_table = pd.concat([pivot_table, totals])

    # Calculate Gap and Gap % between the last two weeks if multiple weeks are present
    weeks = sorted(df[columns_col].unique())
    if len(weeks) > 1:
        last_week = weeks[-1]
        previous_week = weeks[-2]
        pivot_table['Gap'] = pivot_table[last_week] - pivot_table[previous_week]
        pivot_table['Gap %'] = ((pivot_table['Gap'] / pivot_table[previous_week]) * 100).fillna(0).round(1).astype(str) + '%'
    else:
        pivot_table['Gap'] = 0
        pivot_table['Gap %'] = 0

    # Reset index to include the index columns in the final DataFrame
    pivot_table = pivot_table.reset_index()
    
    # Rename the 'index' column to avoid conflict with DataTable
    if 'index' in pivot_table.columns:
        pivot_table = pivot_table.rename(columns={'index': 'Index'})

    return pivot_table
