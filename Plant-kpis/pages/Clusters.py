import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output, MATCH, State, ALL
import pandas as pd
import time
from datetime import datetime
from components import card 
from components.filter import create_filter_dropdown
from components.card import create_card
from data.Clusters import (create_team_segment_clusters,filter_week ,final_df,transform_data,create_summary_dataframe_3, filter_data,create_pivot_table_with_columns,create_summary_dataframe,create_pivot_table)
from config.config import (create_config,format_value)
import logging
import plotly.graph_objs as go
from components.card import create_card, get_figure, create_data_table
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

# Define the function to get the current year
def get_current_year():
    return datetime.now().year

def get_last_week(weeks):
    if not weeks:
        return None
    sorted_weeks = sorted(weeks)
    return sorted_weeks[-1]

def layout():
    current_year = get_current_year()
    return dbc.Container(
        fluid=True,
        children=[
            dcc.Store(id='available-year'),
            dcc.Store(id='available-weeks'),
            dcc.Store(id='dashboardclu-filters'),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Button(
                            children=[
                                "Generate a report",
                                html.Img(src="assets/solar_export-outline.png", className='ms-2', height="20px")
                            ],
                            className="me-2",
                            id="dashboard4-generate-report-btn",
                            style={'fontWeight': 'bold', 'backgroundColor': '#FB8500', 'borderColor': '#FB8500', 'borderRadius': '10px'}
                        ),
                        width="auto",
                        className="d-flex align-items-center"
                    ),
                    dbc.Col(
                        [
                            create_filter_dropdown("Week", 'filter-week', filter_week),
                        ],
                        className="d-flex justify-content-end align-items-center"
                    )
                ],
                className="m-3"
            ),
            dbc.Row(
                [
                    dbc.Col(create_card("AVG demand by project", 1, 'dashboardclu', True, 'Bar', barmode_group=True), width=4, className="p-1"),
                    dbc.Col(create_card("AVG demand $ by project", 2, 'dashboardclu', True, 'Bar', barmode_group=False), width=4, className="p-1"),
                    dbc.Col(create_card("List of clusters by PNs ", 3, 'dashboardclu', False, 'Bar', barmode_group=False,show_switch=False), width=4, className="p-1"),

                ],
                className="m-1"
            ),
               dbc.Row(
                [
                    dbc.Col(create_card("Con by $ ", 4, 'dashboardclu', True, 'Pie', barmode_group=True), width=4, className="p-1"),
                    dbc.Col(create_card("Clusters", 7, 'dashboardclu', False, 'Pie', barmode_group=True,show_switch=False), width=8, className="p-1"),


                ],
                className="m-1"
            ),
                dbc.Row(
                [
                    dbc.Col(create_card("Con by Project", 6, 'dashboardclu', True, 'Bar', barmode_group=True), width=6, className="p-1"),
                    dbc.Col(create_card("Con $ by Project", 5, 'dashboardclu', True, 'Bar', barmode_group=True), width=6, className="p-1"),

                ],
                className="m-1"
            ),
           
        ]
    )

# Register callbacks
def register_callbacks(app):
    @app.callback(
        Output('available-weeks', 'data'),
        [Input('filter-week', 'value')]
    )
    def update_week(filter_week_value):
        weeks_cluster = final_df['Week Number'].unique().tolist()
        weeks_combined = list(set(weeks_cluster))
        weeks_combined.sort()
        return weeks_combined


    @app.callback(
        Output('filter-week', 'options'),
        [Input('available-weeks', 'data')]
    )
    def update_week_dropdown(available_weeks):
        if available_weeks is None:
            return []

        sorted_weeks = sorted(available_weeks)
        return [{'label': str(week), 'value': week} for week in sorted_weeks]

    @app.callback(
        Output('dashboardclu-filters', 'data'),
        [
            Input('filter-week', 'value'),
        ]
    )
    def update_filters( week):
        start_time = time.time()
        if not week:
            week = filter_week
        
        filtered_data = filter_data(week)
        end_time = time.time()
        logging.info(f"Filter update took {end_time - start_time:.2f} seconds")
        logging.info(f"Filtered Data: {filtered_data.head()}")
        
        return filtered_data.to_dict('records')
        
    @app.callback(
        [
            Output({'type': 'dashboardclu-dynamic-content', 'index': MATCH}, 'children'),
            Output({'type': 'dashboardclu-dynamic-content-modal', 'index': MATCH}, 'children'),
        ],
        [
            Input({'type': 'dashboardclu-Graph-switch', 'index': MATCH}, 'value'),
            Input({'type': 'dashboardclu-chart-type', 'index': MATCH}, 'value'),
            Input('dashboardclu-filters', 'data'),
            
        ],
        [
            State({'type': 'dashboardclu-dynamic-content', 'index': MATCH}, 'id'),
            State({'type': 'dashboardclu-barmode-group', 'index': MATCH}, 'data')
        ]
    )
    def update_charts(switch_value, chart_type, filters, component_id, barmode_group):
        index = component_id['index']

        logging.debug(f"Updating charts for index {index}.")

        if not filters:
            logging.warning("No filters found.")
            return dash.no_update, dash.no_update

        # Filter out zero-value days
        filtered_data = pd.DataFrame(filters)
        logging.debug(f"Filtered data for index {index}:\n{filtered_data.head()}")

        if index == 1:
            group_by_cols = ['Project']
            agg_col = 'AVG Demand'
            config_data = create_summary_dataframe(filtered_data, group_by_cols, agg_col)
        elif index == 2:
            group_by_cols = ['Project']
            agg_col = 'AVG Demand $'
            config_data = create_summary_dataframe(filtered_data, group_by_cols, agg_col)
        elif index == 3:
            group_by_cols = ['Material Nbr', 'Project', 'Clusters']
            # Filter out rows where "Project" is blank
            filtered_data = filtered_data[filtered_data['Project'].notna()]
            config_data = filtered_data[group_by_cols].drop_duplicates()
        elif index == 4:
            filtered_data=transform_data(filtered_data, 
                           id_vars=['Week Number'], 
                           value_vars=['Con 0-2 weeks $', 'Con 3-4 weeks $', 'Con 5-6 weeks $', 'Con 7-8 weeks $', 'Con >8 weeks $', 'Obsolete $'], 
                           var_name='Consumption Type', 
                           value_name='Value')
            group_by_cols = ['Consumption Type','Week Number']
            agg_col = 'Value'
            config_data = create_summary_dataframe(filtered_data, group_by_cols, agg_col)
        elif index == 5:
            filtered_data=transform_data(filtered_data, 
                           id_vars=['Project'], 
                           value_vars=['Con 0-2 weeks $', 'Con 3-4 weeks $', 'Con 5-6 weeks $', 'Con 7-8 weeks $', 'Con >8 weeks $', 'Obsolete $'], 
                           var_name='Consumption Type', 
                           value_name='Value')
            group_by_cols = ['Consumption Type','Project']
            agg_col = 'Value'
            config_data = create_summary_dataframe(filtered_data, group_by_cols, agg_col)
            #config_data_Table = create_pivot_table(filtered_data,'Project','Consumption Type', 'Value')
        elif index == 6:
            filtered_data=transform_data(filtered_data, 
                           id_vars=['Project'], 
                           value_vars=['Con 0-2 weeks', 'Con 3-4 weeks', 'Con 5-6 weeks', 'Con 7-8 weeks', 'Con >8 weeks', 'Obsolete'], 
                           var_name='Consumption Type', 
                           value_name='Value')
            group_by_cols = ['Consumption Type','Project']
            agg_col = 'Value'
            config_data = create_summary_dataframe(filtered_data, group_by_cols, agg_col)
        elif index == 7:
            group_by_cols = ['Team', 'Segment']
            agg_col = 'Total Current Value of Stock ($)'
            config_data = create_summary_dataframe(filtered_data, group_by_cols, agg_col)
            config_data_Table = create_team_segment_clusters(filtered_data, ['Team', 'Segment'],'Clusters', 'Total Current Value of Stock ($)')

        else:
            return dash.no_update, dash.no_update
            
        config = create_config(
            config_data,
            config_data_Table if index== 7 else config_data,
            'Project' if index in [1, 2] else 
            'Week Number' if index == 4 else 
            'Project' if index in[5,6]  else None,
            'AVG Demand' if index == 1 else 
            'AVG Demand $' if index == 2 else 'Value' if index in [4,5,6] else None,
            None,
            None,
            'Consumption Type' if index in [4,5,6] else None
        )

        if switch_value:
            fig = get_figure(chart_type, config, "/Clusters", barmode_group=barmode_group)
            logging.debug(f"Generated figure for index {index}.")
            return (
                dcc.Graph(figure=fig),
                dcc.Graph(figure=fig)
            )
        else:
            data = config['dataTable']

            # Apply formatting to the entire DataFrame except for non-numeric columns
            formatted_data = data.copy()
            for col in formatted_data.columns:
                if formatted_data[col].dtype in ['int64', 'float64']:
                    formatted_data[col] = formatted_data[col].apply(format_value)

            style_data_conditional = [
                {
                    'if': {'filter_query': '{Segment} = "Total"'},
                    'backgroundColor': 'rgba(255, 165, 0, 0.2)',
                    'fontWeight': 'bold'
                },
                {
                    'if': {'filter_query': '{Team} = "Grand Total"'},
                    'backgroundColor': 'rgba(0, 255, 0, 0.2)',
                    'fontWeight': 'bold'
                },
                
            ]

            table = create_data_table(formatted_data, '300px', style_data_conditional)
            modal_table = create_data_table(formatted_data, '500px', style_data_conditional, scrollable=True)

            logging.debug(f"Generated table for index {index}.")
            return table, modal_table