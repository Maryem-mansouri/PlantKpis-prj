import pandas as pd
from datetime import datetime
import logging
from data.ETL.ETL_Clusters import final_df 
from  config.config import format_value

# Update filter options
filter_week = final_df['Week Number'].unique().tolist()

print("Filter Week:", filter_week)
# Functions for filtering and summary
def filter_data(week):
    mask = (
        final_df['Week Number'].isin(week) 
    )
    filtered_data = final_df[mask]
    print("Filtered Data inside filter_data function:")
    print(filtered_data.head())
    return filtered_data

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
        # Calculate 'Gap' and 'Gap %' using the actual data from the previous week column
        pivot_table['Gap'] = pivot_table[last_week] - pivot_table[previous_week]
        pivot_table['Gap %'] = ((pivot_table['Gap'] / pivot_table[previous_week]) * 100).fillna(0).round(1).astype(str) + '%'
    else:
        pivot_table['Gap'] = 0
        pivot_table['Gap %'] = '0%'

    # Log pivot table with columns
    logging.info(f"Pivot Table with Columns:\n{pivot_table.head()}")

    return pivot_table

def create_summary_dataframe_3(df, group_by_cols, agg_cols):
    logging.debug(f"Creating summary dataframe with group_by_cols: {group_by_cols} and agg_cols: {agg_cols}")
    
    # Handle Material Nbr aggregation separately if it's one of the columns
    if 'Material Nbr' in agg_cols:
        agg_funcs = {col: 'sum' if col != 'Material Nbr' else lambda x: ', '.join(x) for col in agg_cols}
    else:
        agg_funcs = {col: 'sum' for col in agg_cols}
    
    # Sort and group by specified columns, applying the aggregation functions
    df = df.sort_values(by=group_by_cols)
    summary_df = df.groupby(group_by_cols).agg(agg_funcs).reset_index()
    
    logging.debug(f"Summary DataFrame: {summary_df.head()}")
    return summary_df

def transform_data(df, id_vars, value_vars, var_name='Variable', value_name='Value', filter_zero=True):
    # Melt the DataFrame
    df_melted = df.melt(id_vars=id_vars, 
                        value_vars=value_vars, 
                        var_name=var_name, 
                        value_name=value_name)
    
    # Filter out rows where Value is zero if filter_zero is True
    if filter_zero:
        df_melted = df_melted[df_melted[value_name] != 0]
    
    return df_melted



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

def create_team_segment_clusters(df, index_cols, cluster_col, value_col):
    team_col, segment_col = index_cols

    # Filter out rows with zero values for the given column
    df = df[df[value_col] > 0]

    # Pivot the data by 'Cluster' instead of 'Days'
    pivot_table = df.pivot_table(
        values=value_col,
        index=index_cols,
        columns=cluster_col,
        aggfunc='sum',
        fill_value=0
    ).reset_index()

    # Calculate team totals
    team_totals = pivot_table.groupby(team_col).sum().reset_index()
    team_totals[segment_col] = 'Total'

    # Concatenate team totals with the original pivot table
    summary_with_totals = pd.concat([team_totals, pivot_table], ignore_index=True)

    # Order the columns based on the clusters
    available_columns = [col for col in sorted(df[cluster_col].unique()) if col in summary_with_totals.columns]

    # Create grand total row
    grand_total = pivot_table.sum(numeric_only=True)
    grand_total[team_col] = 'Grand Total'
    grand_total[segment_col] = ''

    final_summary = pd.concat([summary_with_totals, pd.DataFrame(grand_total).T], ignore_index=True)
    final_summary = final_summary[[team_col, segment_col] + available_columns]

    # Sort the final summary by team and segment, with 'Total' rows appearing first
    final_summary = final_summary.sort_values(by=[team_col, segment_col], key=lambda col: col == 'Total', ascending=[True, False])

    # Separate total rows and other rows for easier viewing
    sorted_summary = []
    for team in final_summary[team_col].unique():
        team_data = final_summary[final_summary[team_col] == team]
        total_row = team_data[team_data[segment_col] == 'Total']
        other_rows = team_data[team_data[segment_col] != 'Total']
        sorted_summary.append(total_row)
        sorted_summary.append(other_rows)

    final_sorted_summary = pd.concat(sorted_summary)

    # Remove rows where all values except the first two columns (Team and Segment) are zero
    non_zero_summary = final_sorted_summary.loc[(final_sorted_summary.iloc[:, 2:] != 0).any(axis=1)]

    # Apply formatting (optional, based on your `format_value` function)
    for col in non_zero_summary.columns:
        if col not in [team_col, segment_col]:  # Exclude non-numeric columns from formatting
            non_zero_summary[col] = non_zero_summary[col].apply(format_value)

    return non_zero_summary
