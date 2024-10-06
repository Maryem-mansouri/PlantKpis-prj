import pandas as pd
import numpy as np
from sqlalchemy import create_engine,text

# Step 1: Load the PU data from the Excel file
file_path = r'C:\Users\TE582412\Desktop\PU.xlsx'
pu_df = pd.read_excel(file_path)

# Step 2: Connect to the SQL Server and fetch the necessary tables
conn_str = (
    r'mssql+pyodbc://MAN61NBO1VZ06Y2\SQLEXPRESS/AUTH'
    r'?driver=ODBC+Driver+17+for+SQL+Server'
    r'&trusted_connection=yes&MultipleActiveResultSets=True'
)

engine = create_engine(conn_str)

# Fetch the main EDI data
query_edi = '''
    SELECT [Document]
          ,[Material Nbr]
          ,[Shipping Plant]
          ,[MRP Ctl]
          ,[MRP Name]
          ,[Quantity]
          ,[Bklg Value]
          ,[Curr]
          ,[Cust Req S]
          ,[SoldTo]
          ,[Soldto Name]
          ,[Material Description]
          ,[UnrestUseDelvQty]
          ,[Material-Plant]
          ,[Shipment Month]
          ,[Year]
          ,[Year-Week]
          ,[Week Number]
          ,[Day]
    FROM [Plantkpis_Sp_db].[dbo].[OrderedEDIView]
'''
edi_df = pd.read_sql(query_edi, engine)

# Fetch the Teams, Segments, and Projects data
query_teams = '''
    SELECT [MRP Ctrlr], [Team]
    FROM [Plantkpis_Sp_db].[dbo].[Teams_Table]
'''
teams_df = pd.read_sql(query_teams, engine)

query_segments = '''
    SELECT [Material-Plant], [Segment]
    FROM [Plantkpis_Sp_db].[dbo].[Segments_Table]
'''
segments_df = pd.read_sql(query_segments, engine)

query_projects = '''
    SELECT [Material Number], [Project]
    FROM [Plantkpis_Sp_db].[dbo].[Projects_Table]
'''
projects_df = pd.read_sql(query_projects, engine)

query_inventory = '''
    SELECT [Material], [Total Current Value of Stock ($)], [Week],[Day]
    FROM [Plantkpis_Sp_db].[dbo].[OrderedInventoryView]
'''
inventory_df = pd.read_sql(query_inventory, engine)
# Ensure the Day column is in datetime format for both DataFrames
edi_df['Day'] = pd.to_datetime(edi_df['Day'], errors='coerce')
inventory_df['Day'] = pd.to_datetime(inventory_df['Day'], errors='coerce')

# Step 3: Process for all weeks
unique_weeks = edi_df['Week Number'].unique()
final_df = pd.DataFrame()  # Initialize an empty DataFrame to store the final results

def generate_ship_week_columns(start_week, year):
    ship_weeks = []
    for i in range(0, 25):
        week = start_week + i
        if week <= 52:
            ship_weeks.append(f'{year}-WK{week:02d}')
        else:
            ship_weeks.append(f'{year + 1}-WK{week - 52:02d}')
    return ship_weeks
def calculate_coverage(row, weeks_to_consider):
    # Normal coverage calculation
    stock = row['UnrestUseDelvQty']
    coverage_weeks = 0
    for week in weeks_to_consider:
        if week in row:
            stock -= row[week]
            if stock > 0:
                coverage_weeks += 1
            else:
                break

    return coverage_weeks
def calculate_consumption(row):
    demand_cols = [col for col in row.index if col.startswith('2024-WK') or col.startswith('2025-WK')]
    
    consumption = {}
    
    # Initialize stock variables
    stock_after_first_week = row['UnrestUseDelvQty'] - row[demand_cols[0]]
    stock_after_second_week = stock_after_first_week - row[demand_cols[1]]
    stock_after_fourth_week = stock_after_second_week - row[demand_cols[2]] - row[demand_cols[3]]
    stock_after_sixth_week = stock_after_fourth_week - row[demand_cols[4]] - row[demand_cols[5]]
    stock_after_eighth_week = stock_after_sixth_week - row[demand_cols[6]] - row[demand_cols[7]]

    # Con 0-2 weeks
    if row['Total Quantity'] == 0:
        consumption['Con 0-2 weeks'] = 0
    else:
        if stock_after_second_week > 0:
            consumption['Con 0-2 weeks'] = row[demand_cols[0]] + row[demand_cols[1]]
        else:
            consumption['Con 0-2 weeks'] = row['UnrestUseDelvQty']

    # Con 3-4 weeks
    if row['Total Quantity'] == 0:
        consumption['Con 3-4 weeks'] = 0
    else:
        if stock_after_fourth_week > 0:
            consumption['Con 3-4 weeks'] = row[demand_cols[2]] + row[demand_cols[3]]
        else:
            consumption['Con 3-4 weeks'] = row['UnrestUseDelvQty'] -  consumption['Con 0-2 weeks']

    # Con 5-6 weeks
    if row['Total Quantity'] == 0:
        consumption['Con 5-6 weeks'] = 0
    else:
        if stock_after_sixth_week > 0:
            consumption['Con 5-6 weeks'] = row[demand_cols[4]] + row[demand_cols[5]]
        else:
            consumption['Con 5-6 weeks'] = row['UnrestUseDelvQty'] -  consumption['Con 3-4 weeks'] - consumption['Con 0-2 weeks']

    # Con 7-8 weeks
    if row['Total Quantity'] == 0:
        consumption['Con 7-8 weeks'] = 0
    else:
        if stock_after_eighth_week > 0:
            consumption['Con 7-8 weeks'] = row[demand_cols[6]] + row[demand_cols[7]]
        else:
            consumption['Con 7-8 weeks'] = row['UnrestUseDelvQty'] - consumption['Con 5-6 weeks'] - consumption['Con 3-4 weeks'] - consumption['Con 0-2 weeks']

    # Con >8 weeks
    if row['Total Quantity'] == 0:
        consumption['Con >8 weeks'] = 0
    else:
        total_previous_consumption = sum([consumption['Con 0-2 weeks'], consumption['Con 3-4 weeks'], consumption['Con 5-6 weeks'], consumption['Con 7-8 weeks']])
        consumption['Con >8 weeks'] = row['UnrestUseDelvQty'] - total_previous_consumption

    # Obsolete
    if row['Total Quantity'] == 0:
        consumption['Obsolete'] = row['UnrestUseDelvQty']
    else:
        consumption['Obsolete'] = 0
    
    return pd.Series(consumption)

# Step 3: Define a function to backfill missing stock values selectively
def backfill_stock_values(merged_df, inventory_df, week):
    # Get inventory data for the specific week
    inventory_week_df = inventory_df[inventory_df['Week'] == week]
    
    # Find rows where no matching 'Day' in inventory exists
    missing_stock_mask = merged_df['Total Current Value of Stock ($)'].isnull()
    
    # Apply forward-fill only on those missing stock rows, but only for the current week
    merged_df.loc[missing_stock_mask, 'Total Current Value of Stock ($)'] = merged_df.loc[missing_stock_mask].groupby('Material Nbr')['Total Current Value of Stock ($)'].fillna(method='ffill')
    
    return merged_df

for week in unique_weeks:
    # Step 4: Filter data for the current week
    edi_week_df = edi_df[edi_df['Week Number'] == week].copy()
    
    # Select relevant columns and create the pivot table
    final_week_df = edi_week_df[['Week Number', 'Material Nbr', 'UnrestUseDelvQty', 'Quantity', 'Cust Req S', 'Material-Plant', 'MRP Ctl','Day']].copy()

    # Calculate Ship Week in the format 'Year-WKWeekNumber'
    final_week_df['Ship Week'] = pd.to_datetime(final_week_df['Cust Req S']).dt.strftime('%Y-WK%U')

    # Drop 'Cust Req S' column if it's not needed
    final_week_df.drop(columns=['Cust Req S'], inplace=True)

    # Pivot the data to distribute 'Quantity' across 'Ship Week'
    pivot_week_df = final_week_df.pivot_table(
        index=['Week Number', 'Material Nbr', 'UnrestUseDelvQty', 'Material-Plant', 'MRP Ctl'],
        columns='Ship Week',
        values='Quantity',
        aggfunc='sum',
        fill_value=0
    ).reset_index()

    # After pivoting, add the 'Day' column back from edi_week_df if needed
    pivot_week_df['Day'] = edi_week_df['Day']

    # Generate the column names for the next 24 weeks starting from CW (Current Week)
    current_year = 2024
    ship_week_columns = generate_ship_week_columns(week, current_year)

    # Filter columns that exist in the DataFrame for the current week
    available_columns = ['Week Number', 'Material Nbr', 'UnrestUseDelvQty', 'Material-Plant', 'MRP Ctl', 'Day'] + [col for col in ship_week_columns if col in pivot_week_df.columns]
    filtered_week_df = pivot_week_df[available_columns].copy()

    # Add the sum of quantities for the filtered weeks
    filtered_week_df['Total Quantity'] = filtered_week_df[available_columns[6:]].sum(axis=1)

    # Merge with Teams, Segments, and Projects data after pivoting
    filtered_week_df = filtered_week_df.merge(teams_df, how='left', left_on='MRP Ctl', right_on='MRP Ctrlr')
    filtered_week_df.drop(columns=['MRP Ctrlr'], inplace=True)

    filtered_week_df = filtered_week_df.merge(segments_df, how='left', on='Material-Plant')
    filtered_week_df = filtered_week_df.merge(projects_df, how='left', left_on='Material Nbr', right_on='Material Number')
    filtered_week_df.drop(columns=['Material Number'], inplace=True)
 
    #  Filter inventory data for the current week and merge**
    inventory_week_df = inventory_df[inventory_df['Week'] == week].copy()
    filtered_week_df = filtered_week_df.merge(
    inventory_week_df,
    how='left',
    left_on=['Material Nbr', 'Day'],  # Match on both 'Material Nbr' and 'Day' from the EDI data
    right_on=['Material', 'Day']  # Match on both 'Material' and 'Day' from the inventory data
)
     # Step 6: Backfill missing stock values for this week
    filtered_week_df = backfill_stock_values(filtered_week_df, inventory_df, week)
    
    # Step 7: Remove duplicates and keep only one stock value per material per week
    filtered_week_df.drop_duplicates(subset=['Material Nbr', 'Week Number', 'Day'], inplace=True)
    # Display the resulting merged DataFrame
    print(filtered_week_df)

   
    # Clean up the merged columns by dropping unnecessary ones and renaming where needed
    filtered_week_df.drop(columns=['Material'], inplace=True)   

    # Check for duplicate columns and drop if necessary
    filtered_week_df = filtered_week_df.loc[:, ~filtered_week_df.columns.duplicated()]

    # Define and apply the coverage calculation function
    weeks_to_consider = [col for col in filtered_week_df.columns if col.startswith('2024-WK') or col.startswith('2025-WK')]
    filtered_week_df['Coverage'] = filtered_week_df.apply(lambda row: calculate_coverage(row, weeks_to_consider), axis=1)

    # Merge PU data
    filtered_week_df = filtered_week_df.merge(
        pu_df[['Material Nbr', 'PU']],  # Only merging the relevant columns
        how='left',
        on='Material Nbr'  # Merge on 'Material Nbr'
    )

    # Calculate the AVG Demand and AVG Demand $
    weekly_demand_columns = [col for col in filtered_week_df.columns if col.startswith('2024-WK') or col.startswith('2025-WK')]
    filtered_week_df['AVG Demand'] = filtered_week_df[weekly_demand_columns].mean(axis=1)
    filtered_week_df['AVG Demand $'] = filtered_week_df['AVG Demand'] * filtered_week_df['PU']

    # Define conditions for Clusters
    conditions = [
        filtered_week_df['Coverage'] <= 2,
        (filtered_week_df['Coverage'] > 2) & (filtered_week_df['Coverage'] <= 4),
        (filtered_week_df['Coverage'] > 4) & (filtered_week_df['Coverage'] <= 8),
        (filtered_week_df['Coverage'] > 8) & (filtered_week_df['Coverage'] <= 12),
        (filtered_week_df['Coverage'] > 12) & (filtered_week_df['Coverage'] <= 24),
        filtered_week_df['Coverage'] > 24,
        (filtered_week_df['Total Quantity'] == 0)
    ]
    cluster_labels = [
        "0-2 weeks",
        "3-4 weeks",
        "5-8 weeks",
        "9-12 weeks",
        ">12 weeks",
        ">6 months",
        "Obsolete"
    ]
    filtered_week_df['Clusters'] = np.select(conditions, cluster_labels,default='Unknown')

    # Print columns of final_df before the calculation
    print("Columns in final_df before the consumption calculation:", final_df.columns)

    # Apply the consumption calculation
    consumption_week_df = filtered_week_df.apply(calculate_consumption, axis=1)

    # Concatenate the consumption columns with final_df
    final_week_df = pd.concat([filtered_week_df, consumption_week_df], axis=1)
    final_week_df.reset_index(drop=True, inplace=True)

    # Check the columns again
    print("Columns in final_df after the consumption calculation:", final_week_df.columns)

    # Append the result for this week to the final DataFrame
    final_df = pd.concat([final_df, final_week_df], ignore_index=True)

# Ensure final_df has no duplicate indices
final_df.drop_duplicates(inplace=True)
final_df.reset_index(drop=True, inplace=True)


# Calculate the Con $ values
for period in ['Con 0-2 weeks', 'Con 3-4 weeks', 'Con 5-6 weeks', 'Con 7-8 weeks', 'Con >8 weeks', 'Obsolete']:
    final_df[f'{period} $'] = final_df[period] * final_df['PU']

# The final DataFrame is now ready, without dynamic "Ship Week" columns
final_columns = [col for col in final_df.columns if not col.startswith('2024-WK') and not col.startswith('2025-WK')]
final_df = final_df[final_columns]

# Save the final DataFrame to an Excel file
#output_file_path = r'C:\Users\TE582412\Desktop\Final_Pivoted_DataFrame_with_Consumption_and_Obsolete_with_Prices_Stock_allweeks_day.xlsx'
#final_df.to_excel(output_file_path, index=False)
#print(f"Final DataFrame for all weeks has been saved to {output_file_path}")
