
import pandas as pd
import logging
from sqlalchemy import create_engine, select, func, Table, MetaData
from sqlalchemy.sql import text
from config.config import format_value
from app.db import engine, metadata ,data_store
import sys
sys.path.append("C:/Users/TE582412/Desktop/Structured Plant Kpis/PlantKpis")

from .ETL.Inventory_db_ETL import extract_year_week , inventory_dir
import os

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the tables
inventory_table = Table('Inventory', metadata, autoload_with=engine)
purchased_parts_table = Table('PurchasedParts', metadata, autoload_with=engine)


def get_filter_options():
    with engine.connect() as connection:
        # Fetch distinct plants, years, and days from the data source
        filter_plant = [row[0] for row in connection.execute(select(inventory_table.c.Plant).distinct())]
        filter_year = [row[0] for row in connection.execute(select(inventory_table.c.Year).distinct())]
        
        # Get distinct weeks from the database
        db_weeks = [row[0] for row in connection.execute(select(inventory_table.c.Week).distinct())]
        
        # Get existing folders in the data directory
        existing_folders = [f for f in os.listdir(inventory_dir) if os.path.isdir(os.path.join(inventory_dir, f))]
        
        # Extract weeks from the folder names
        existing_weeks = [extract_year_week(folder)[1] for folder in existing_folders]
        
        # Only keep weeks that exist both in the database and in the folders
        filter_week = sorted(set(db_weeks).intersection(existing_weeks))
        
        # Fetch distinct days from the data source
        filter_day = [row[0] for row in connection.execute(select(inventory_table.c.Day).distinct())]

    return filter_plant, filter_year, filter_week, filter_day

filter_plant, filter_year, filter_week, filter_day = get_filter_options()

# Define a custom order for the days of the week
days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']

# Fetch data based on filters
def fetch_data(plant, year, week, day, table_name='Inventory'):
    logging.debug(f"Fetching data for Plant: {plant}, Year: {year}, Week: {week}, Day: {day}")
    df = data_store[table_name.lower()]
    df_filtered = df[
        (df['Plant'].isin(plant)) &
        (df['Year'].isin(year)) &
        (df['Week'].isin(week)) &
        (df['Day'].isin(day))
    ]
    
    # Remove days with no data
    df_filtered = df_filtered[df_filtered['Total Current Value of Stock ($)'] > 0]
    
    logging.debug(f"Fetched data: {df_filtered.head()}")
    return df_filtered




def aggregate_to_weekly(df):
    df['Day_Part'] = df['Day'].apply(lambda x: x.split('-')[1])
    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    df['Day_Part'] = pd.Categorical(df['Day_Part'], categories=days_order, ordered=True)
    last_day_of_week = df.groupby(['Year', 'Week'])['Day_Part'].transform('max')
    weekly_df = df[df['Day_Part'] == last_day_of_week]
    weekly_df['Week'] = weekly_df['Week'].apply(lambda x: f'WK{x}')
    return weekly_df

def create_gap_column(df, group_by_cols, value_col):
    df = df.sort_values(by=['Week'] + group_by_cols)
    df['Previous Week'] = df.groupby(group_by_cols)[value_col].shift(1)
    df['Gap'] = df[value_col] - df['Previous Week']
    return df

def create_summary_dataframe(df, group_by_cols, agg_cols):
    logging.debug(f"Creating summary dataframe with group_by_cols: {group_by_cols} and agg_cols: {agg_cols}")
    if isinstance(agg_cols, str):
        agg_cols = {agg_cols: 'sum'}
    elif not isinstance(agg_cols, dict):
        raise ValueError("agg_cols should be a dictionary with columns and their respective aggregation functions.")
    
    df = df.sort_values(by=group_by_cols)
    summary_df = df.groupby(group_by_cols).agg(agg_cols).reset_index()
    logging.debug(f"Summary DataFrame: {summary_df.head()}")
    return summary_df


def create_pivot_table(df, index_col, columns_col, values_col):
    # Define a custom order for the days of the week
    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    
    # Extract the day part and week part from the 'Day' column
    df['Week_Day'] = df['Day'].apply(lambda x: '-'.join(x.split('-')[:2]) if '-' in x else x)
    df['Day_Part'] = df['Day'].apply(lambda x: x.split('-')[1] if '-' in x else x)
    df['Day_Part'] = pd.Categorical(df['Day_Part'], categories=days_order, ordered=True)
    
    df = df[df[values_col] > 0]
    # Sort the DataFrame by the 'Day_Part' column
    df = df.sort_values('Day_Part')

    # Handle multi-level index columns
    if isinstance(index_col, list):
        pivot_table = df.pivot_table(
            values=values_col,
            index=index_col,
            columns='Week_Day',
            aggfunc='sum',
            fill_value=0
        )
    else:
        pivot_table = df.pivot_table(
            values=values_col,
            index=[index_col],
            columns='Week_Day',
            aggfunc='sum',
            fill_value=0
        )
    
    # Calculate the totals for each week (column)
    totals = pivot_table.sum(axis=0).to_frame().T
    if isinstance(index_col, list):
        totals[index_col] = ['Total'] * len(index_col)
        totals = totals.set_index(index_col)
    else:
        totals[index_col] = 'Total'
        totals = totals.set_index([index_col])
    
    # Append the totals row to the pivot table using pd.concat
    pivot_table = pd.concat([pivot_table, totals])
    
    # Calculate the gap (last week - previous week)
    weeks = sorted(df['Week_Day'].unique())
    if len(weeks) > 1:
        last_week = weeks[-1]
        previous_week = weeks[-2]
        pivot_table['Gap'] = pivot_table[last_week] - pivot_table[previous_week]
        pivot_table['Gap %'] = ((pivot_table['Gap'] / pivot_table[previous_week]) * 100).fillna(0).round(1).astype(str) + '%'
    else:
        pivot_table['Gap'] = 0
        pivot_table['Gap %'] = '0%'

    # Reset index to get the Segment as columns
    pivot_table = pivot_table.reset_index()

    # Check if more than one week is selected
    unique_weeks = df['Week_Day'].apply(lambda x: x.split('-')[0]).unique()
    if len(unique_weeks) > 1:
        ordered_columns = [f"{week}" for week in sorted(set(df['Week_Day'].apply(lambda x: x.split('-')[0])))]
    else:
        ordered_columns = [f"{week}-{day}" for week in sorted(set(df['Week_Day'].apply(lambda x: x.split('-')[0]))) for day in days_order]

    if isinstance(index_col, list):
        pivot_table = pivot_table[[col for col in index_col + ordered_columns + ['Gap', 'Gap %'] if col in pivot_table.columns]]
    else:
        pivot_table = pivot_table[[col for col in [index_col] + ordered_columns + ['Gap', 'Gap %'] if col in pivot_table.columns]]
    
    return pivot_table

def create_pivot_table_with_columns(df, columns, values):
    # Define a custom order for the days of the week
    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    
    # Extract the day part and week part from the 'Day' column
    df['Week_Day'] = df['Day'].apply(lambda x: '-'.join(x.split('-')[:2]) if '-' in x else x)
    df['Day_Part'] = df['Day'].apply(lambda x: x.split('-')[1] if '-' in x else x)
    df['Day_Part'] = pd.Categorical(df['Day_Part'], categories=days_order, ordered=True)
    
    # Remove rows with no values
    df = df[df[values] > 0]
    # Sort the DataFrame by the 'Day_Part' column
    df = df.sort_values('Day_Part')

    # Create the pivot table
    pivot_table = df.pivot_table(
        values=values,
        columns='Week_Day',
        aggfunc='sum',
        fill_value=0
    ).reset_index()

    # Calculate the gap (last week - previous week)
    weeks = [col for col in pivot_table.columns if col != 'index']  # assuming 'index' is the first column
    if len(weeks) > 1:
        last_week = weeks[-1]
        previous_week = weeks[-2]
        pivot_table['Gap'] = pivot_table[last_week] - pivot_table[previous_week]
        pivot_table['Gap %'] = ((pivot_table['Gap'] / pivot_table[previous_week]) * 100).fillna(0).round(1).astype(str) + '%'
    else:
        pivot_table['Gap'] = 0
        pivot_table['Gap %'] = '0%'

    # Reorder columns based on the presence of multiple weeks or not
    unique_weeks = df['Week_Day'].apply(lambda x: x.split('-')[0]).unique()
    if len(unique_weeks) > 1:
        ordered_columns = [f"{week}" for week in sorted(set(df['Week_Day'].apply(lambda x: x.split('-')[0])))]
    else:
        ordered_columns = [f"{week}-{day}" for week in sorted(set(df['Week_Day'].apply(lambda x: x.split('-')[0]))) for day in days_order]

    pivot_table = pivot_table[[col for col in ['index'] + ordered_columns + ['Gap', 'Gap %'] if col in pivot_table.columns]]

    return pivot_table


def create_team_segment_daily_pivot(df, index_cols, day_col, value_col, days_order=None):
    team_col, segment_col = index_cols

    if days_order is None:
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']

    # Create Week_Day and Day_Part columns
    df['Week_Day'] = df[day_col].apply(lambda x: '-'.join(x.split('-')[:2]) if '-' in x else x)
    df['Day_Part'] = df[day_col].apply(lambda x: x.split('-')[1] if '-' in x else x)
    df['Day_Part'] = pd.Categorical(df['Day_Part'], categories=days_order, ordered=True)
    df = df.sort_values('Day_Part')

    # Filter out days with no data
    df = df[df[value_col] > 0]
    
    pivot_table = df.pivot_table(
        values=value_col,
        index=index_cols,
        columns='Week_Day',
        aggfunc='sum',
        fill_value=0
    ).reset_index()

    team_totals = pivot_table.groupby(team_col).sum().reset_index()
    team_totals[segment_col] = 'Total'

    summary_with_totals = pd.concat([team_totals, pivot_table], ignore_index=True)

    # Ensure all expected columns are present
    unique_weeks = df['Week_Day'].apply(lambda x: x.split('-')[0]).unique()
    if len(unique_weeks) > 1:
        ordered_columns = [f"{week}" for week in sorted(unique_weeks)]
    else:
        ordered_columns = [f"{week}-{day}" for week in unique_weeks for day in days_order]

    available_columns = [col for col in ordered_columns if col in summary_with_totals.columns]
    
    gap_col = 'Gap'
    gap_percent_col = 'Gap %'

    if len(available_columns) > 1:
        summary_with_totals[gap_col] = summary_with_totals[available_columns[-1]] - summary_with_totals[available_columns[-2]]
        summary_with_totals[gap_percent_col] = ((summary_with_totals[gap_col] / summary_with_totals[available_columns[-2]]) * 100).fillna(0).round(1).astype(str) + '%'
    else:
        summary_with_totals[gap_col] = 0
        summary_with_totals[gap_percent_col] = '0%'

    grand_total = pivot_table.sum(numeric_only=True)
    grand_total[team_col] = 'Grand Total'
    grand_total[segment_col] = ''

    final_summary = pd.concat([summary_with_totals, pd.DataFrame(grand_total).T], ignore_index=True)
    final_summary = final_summary[[team_col, segment_col] + available_columns + [gap_col, gap_percent_col]]

    final_summary = final_summary.sort_values(by=[team_col, segment_col], key=lambda col: col == 'Total', ascending=[True, False])

    sorted_summary = []
    for team in final_summary[team_col].unique():
        team_data = final_summary[final_summary[team_col] == team]
        total_row = team_data[team_data[segment_col] == 'Total']
        other_rows = team_data[team_data[segment_col] != 'Total']
        sorted_summary.append(total_row)
        sorted_summary.append(other_rows)

    final_sorted_summary = pd.concat(sorted_summary)

    # Remove rows where all values except the first two columns (Team and Segment) and the last two columns (Gap and Gap %) are zero
    non_zero_summary = final_sorted_summary.loc[(final_sorted_summary.iloc[:, 2:-2] != 0).any(axis=1)]

    # Apply formatting
    for col in non_zero_summary.columns:
        if col not in [team_col, segment_col, gap_percent_col]:  # Exclude non-numeric columns from formatting
            non_zero_summary[col] = non_zero_summary[col].apply(format_value)

    return non_zero_summary


# Ensure the pivot table columns are explicitly ordered
def reorder_columns(pivot_table, days_order):
    ordered_columns = [f"WK{week}-{day}" for week in sorted(set(pivot_table.columns.get_level_values(0).str.split('-').str[0])) for day in days_order]
    other_columns = [col for col in pivot_table.columns if col not in ordered_columns]
    return pivot_table[other_columns + ordered_columns]

