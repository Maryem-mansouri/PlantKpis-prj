import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, MATCH, State,ALL
import pandas as pd
import logging
from components.card import create_card, get_figure, create_data_table
from components.report import generate_pdf_report
from components.valuecard import create_value_card
from datetime import datetime
import plotly.io as pio
import io
import base64
from app.db import refresh_data_store
from data.Inventory_db import (
     get_filter_options, fetch_data, engine, 
    aggregate_to_weekly, create_team_segment_daily_pivot, create_pivot_table_with_columns, 
    create_pivot_table, create_gap_column, filter_week, filter_plant, filter_day, filter_year, 
    create_summary_dataframe)
from config.config import create_config, format_value
from sqlalchemy import select, func
from app.db import engine, metadata,data_store

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

figures=[None for __ in range(8)]

def get_current_year():
    return datetime.now().year

def get_latest_week():
    with engine.connect() as connection:
        latest_week = connection.execute(
            select(func.max(metadata.tables['Inventory'].c.Week))
        ).scalar()
    return latest_week

def create_filter_dropdown(label, checklist_id, options, default_value=None):
    return dbc.DropdownMenu(
        label=label,
        children=[
            dbc.DropdownMenuItem(
                html.Div(
                    dbc.Checklist(
                        id=checklist_id,
                        options=[{'label': option, 'value': option} for option in options],
                        value=default_value if default_value else options,
                        inline=False,
                    ),
                    style={'maxHeight': '200px', 'overflowY': 'auto'}
                ),
                toggle=False,
                className="bg-white"
            ),
        ],
        color="light",
        className="me-2"
    )

def layout():
    filter_plant, filter_year, filter_week, filter_day = get_filter_options()
    current_year = get_current_year()
    latest_week = get_latest_week()
    return dbc.Container(
        fluid=True,
        children=[
            dcc.Store(id='available-years'),
            dcc.Store(id='available-weeks-inventorydb'),
            dcc.Store(id='dashboardinv-filters'),
            dcc.Store(id='dashboardinv-purchased-parts-filters'),
            dbc.Row(
                [
                    dbc.Col(
                        
                        dbc.Button(
                            children=[
                                "Generate a report",
                                html.Img(src="assets/solar_export-outline.png", className='ms-2', height="20px")
                            ],
                            className="me-2",
                            id="dashboardinv-generate-report-btn",
                            style={'fontWeight': 'bold', 'backgroundColor': '#FB8500', 'borderColor': '#FB8500', 'borderRadius': '10px'}
                        ),
                        width="auto",
                        className="d-flex align-items-center"
                    ),
                    dbc.Col(
                            html.A(
                                dbc.Button(
                                    "Download Report",
                                    className="download-button",
                                    style={'fontWeight': 'bold', 'backgroundColor': '#4CAF50', 'borderColor': '#4CAF50',
                                           'borderRadius': '10px'}
                                ),
                                id="dashboardinv-download-link",
                                href="/download_report",
                                className="me-2",
                                style={'display': 'none'},  # Hidden initially
                                download="generated_report.pdf"
                            ),
                            width="auto",
                             ),          
                         dbc.Col(
                        [
                            create_filter_dropdown("Plant", 'filter-location', filter_plant),
                            create_filter_dropdown("Year", 'filter-fiscal-year', filter_year, [current_year]),
                            create_filter_dropdown("Week", 'filter-week-inventorydb', filter_week, [latest_week]),
                            create_filter_dropdown("Day", 'filter-daydb', filter_day),
                        ],
                        className="d-flex justify-content-end align-items-center"
                    ),
                ],
                className="m-3"
            ),
            dbc.Row(
                [
                    dbc.Col(create_card("Inventory $ by Project", 1, 'dashboardinv', True, 'Bar', barmode_group=False), width=4, className="p-1"),
                    dbc.Col(create_card("inventory $ by plant", 2, 'dashboardinv', True, 'Pie'), width=4, className="p-1"),
                    dbc.Col(create_card("inventory $ trend", 3, 'dashboardinv', True, 'Line'), width=4, className="p-1")
                ],
                className="m-1"
            ),
            dbc.Row(
                [
                    dbc.Col(create_card("Inventory $ by Team & Seg", 4, 'dashboardinv', False, 'Bar', barmode_group=False, show_switch=False), width=8, className="p-1"),
                    dbc.Col(create_card(" Today's Inventory value ", 5, 'dashboardinv', True, 'Bar', barmode_group=False, show_switch=False), width=4, className="p-1")
                ],
                className="m-1"
            ),
            dbc.Row(
                [
                    dbc.Col(create_card("CAS Inventory value ", 9, 'dashboardinv', True, 'Bar', barmode_group=False), width=4, className="p-1"),
                    dbc.Col(create_card("MCA Inventory value", 7, 'dashboardinv', True, 'Bar', barmode_group=False), width=4, className="p-1"),
                    dbc.Col(create_card("Stamping Inventory value", 8, 'dashboardinv', True, 'Bar', barmode_group=False), width=4, className="p-1")
                ],
                className="m-1"
            ),
            dbc.Row(
                [
                    dbc.Col(create_card("Top 10 vendors", 6, 'dashboardinv', True, 'Bar', barmode_group=True), width=12, className="p-1"),
                ],
                className="m-1"
            ),
        ]
    )

def register_callbacks(app):
    @app.callback(
    Output('available-weeks-inventorydb', 'data'),
    [Input('filter-fiscal-year', 'value')]
)
    def update_week_based_on_year(selected_years):
        if not selected_years:
            selected_years = [get_current_year()]
    
        refresh_data_store()  # Ensure the data store is up to date
    
        filtered_df_inventory = data_store['inventory'][data_store['inventory']['Year'].isin(selected_years)]
        filtered_df_purchased_parts = data_store['purchased_parts'][data_store['purchased_parts']['Year'].isin(selected_years)]
    
        # Filter out weeks that do not have any data
        weeks_inventory = filtered_df_inventory['Week'].unique().tolist()
        weeks_purchased_parts = filtered_df_purchased_parts['Week'].unique().tolist()
    
        weeks_combined = sorted(set(weeks_inventory).intersection(weeks_purchased_parts))  # Only keep weeks present in both tables
        logging.debug(f"Weeks in Inventory: {weeks_inventory}, Weeks in PurchasedParts: {weeks_purchased_parts}")
        logging.debug(f"Weeks to be displayed: {weeks_combined}")
        return weeks_combined


    @app.callback(
        Output('filter-week-inventorydb', 'options'),
        [Input('available-weeks-inventorydb', 'data')]
    )
    def update_week_dropdown(available_weeks):
        if available_weeks is None:
            return []
        return [{'label': week, 'value': week} for week in available_weeks]

    @app.callback(
        Output('filter-daydb', 'options'),
        [Input('filter-week-inventorydb', 'value')]
    )
    def update_day_dropdown(selected_weeks):
        if not selected_weeks:
            return []
    
        filtered_days = data_store['inventory'][data_store['inventory']['Week'].isin(selected_weeks)]['Day'].unique().tolist()
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
        sorted_days = sorted(filtered_days, key=lambda x: days_order.index(x.split('-')[1]))

        return [{'label': day, 'value': day} for day in sorted_days]

    @app.callback(
        Output('dashboardinv-filters', 'data'),
        [
            Input('filter-location', 'value'),
            Input('filter-fiscal-year', 'value'),
            Input('filter-week-inventorydb', 'value'),
            Input('filter-daydb', 'value'),
        ]
    )
    def update_filters(loc, year, week, day):
        if not year:
            year = [get_current_year()]
        
        filtered_data = fetch_data(loc, year, week, day)
        return filtered_data.to_dict('records')

    @app.callback(
        Output('dashboardinv-purchased-parts-filters', 'data'),
        [
            Input('filter-location', 'value'),
            Input('filter-fiscal-year', 'value'),
            Input('filter-week-inventorydb', 'value'),
            Input('filter-daydb', 'value'),
        ]
    )
    def update_purchased_parts_filters(loc, year, week, day):
        if not year:
            year = [get_current_year()]
        
        filtered_data = fetch_data(loc, year, week, day,'purchased_parts')
        return filtered_data.to_dict('records')

    @app.callback(
        [
            Output({'type': 'dashboardinv-dynamic-content', 'index': MATCH}, 'children'),
            Output({'type': 'dashboardinv-dynamic-content-modal', 'index': MATCH}, 'children'),
        ],
        [
            Input({'type': 'dashboardinv-Graph-switch', 'index': MATCH}, 'value'),
            Input({'type': 'dashboardinv-chart-type', 'index': MATCH}, 'value'),
            Input('dashboardinv-filters', 'data'),
            Input('dashboardinv-purchased-parts-filters', 'data'),
            Input('filter-week-inventorydb', 'value')
        ],
        [
            State({'type': 'dashboardinv-dynamic-content', 'index': MATCH}, 'id'),
            State({'type': 'dashboardinv-barmode-group', 'index': MATCH}, 'data')
        ]
    )
    def update_charts(switch_value, chart_type, filters, purchased_parts_filters, selected_weeks, component_id, barmode_group):
        index = component_id['index']

        logging.debug(f"Updating charts for index {index}.")

        if not filters:
            logging.warning("No filters found.")
            return dash.no_update, dash.no_update
        
          # Filter out zero-value days
        filtered_data = pd.DataFrame(filters)
        logging.debug(f"Filtered data for index {index}:\n{filtered_data.head()}")
        filtered_data = filtered_data[filtered_data['Total Current Value of Stock ($)'] > 0]

        # Check if more than one week is selected
        if len(selected_weeks) > 1:
            filtered_data = aggregate_to_weekly(filtered_data)
            filtered_data['Day'] = filtered_data['Week']
        else:
            days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
            existing_days = filtered_data['Day'].unique()
            filtered_data = filtered_data[filtered_data['Day'].isin(existing_days)]
            # Apply the correct order to the days
            filtered_data['Day'] = pd.Categorical(filtered_data['Day'], categories=[f'WK{selected_weeks[0]}-{day}' for day in days_order if f'WK{selected_weeks[0]}-{day}' in existing_days], ordered=True)


        # Define group_by_cols and agg_col based on the index
        if index == 6:
            filtered_data = pd.DataFrame(purchased_parts_filters)
            if len(selected_weeks) > 1:
                filtered_data = aggregate_to_weekly(filtered_data)
                filtered_data['Day'] = filtered_data['Week']
            group_by_cols = ['Day', 'vendor Name / plant']
            agg_col = 'Total Current Value of Stock ($)'
            logging.debug(f"Group by columns: {group_by_cols}, Aggregation column: {agg_col}")
        else:
            if index == 1:
                group_by_cols = ['Day', 'Project']
                agg_col = 'Total Current Value of Stock ($)'
            elif index == 2:
                group_by_cols = ['Day', 'Plant']
                agg_col = 'Total Current Value of Stock ($)'
            elif index == 3:
                group_by_cols = ['Day']
                agg_col = 'Total Current Value of Stock ($)'
            elif index == 4:
                group_by_cols = ['Team', 'Segment']
                agg_col = 'Total Current Value of Stock ($)'
            elif index == 5:
                group_by_cols = ['Team', 'Segment']
                agg_col = 'Total Current Value of Stock ($)'
                # Filter filtered_data to include only the last day in the selected week
                if selected_weeks:
                    last_day = filtered_data['Day'].max()
                    filtered_data = filtered_data[filtered_data['Day'] == last_day]
                    logging.debug(f"Filtered data for the last day {last_day} in the selected weeks:\n{filtered_data.head()}")
            elif index == 9:
                group_by_cols = ['Day', 'Segment']
                agg_col = 'Total Current Value of Stock ($)'
                filtered_data = filtered_data[filtered_data['Team'] == 'CAS']
            elif index == 7:
                group_by_cols = ['Day', 'Segment']
                agg_col = 'Total Current Value of Stock ($)'
                filtered_data = filtered_data[filtered_data['Team'] == 'MCA']
            elif index == 8:
                group_by_cols = ['Day', 'Segment']
                agg_col = 'Total Current Value of Stock ($)'
                filtered_data = filtered_data[filtered_data['Team'] == 'Stamping']
            else:
                return dash.no_update, dash.no_update

        # Calculate summary dataframe
        if index == 3:
            config_data = create_summary_dataframe(filtered_data, group_by_cols, agg_col)
            config_data_table = create_pivot_table_with_columns(filtered_data, 'Day', 'Total Current Value of Stock ($)')
        if index == 1:
            config_data = create_summary_dataframe(filtered_data, group_by_cols, agg_col)
            config_data_table = create_pivot_table(filtered_data, 'Project', 'Day', 'Total Current Value of Stock ($)')
        if index == 2:
            config_data = create_summary_dataframe(filtered_data, group_by_cols, agg_col)
            config_data_table = create_pivot_table(filtered_data, 'Plant', 'Day', 'Total Current Value of Stock ($)')
        if index == 6:
            config_data = create_summary_dataframe(filtered_data, group_by_cols, agg_col)
            config_data_top10 = config_data.sort_values(by=agg_col, ascending=False).groupby('Day').head(10)
            config_data_table = create_pivot_table(filtered_data, 'vendor Name / plant', 'Day', 'Total Current Value of Stock ($)')
            logging.debug(f"Top 10 vendors for each day:\n{config_data_top10}")
        if index == 4:
            config_data = create_summary_dataframe(filtered_data, group_by_cols, agg_col)
            config_data_table = create_team_segment_daily_pivot(filtered_data, ['Team', 'Segment'], 'Day', 'Total Current Value of Stock ($)')
        if index in [7, 8, 9]:
            config_data = create_summary_dataframe(filtered_data, group_by_cols, agg_col)
            config_data_table = create_pivot_table(filtered_data, 'Segment', 'Day', 'Total Current Value of Stock ($)')
        if index == 5:
            config_data = create_summary_dataframe(filtered_data, group_by_cols, agg_col)
            config_data_table = create_team_segment_daily_pivot(filtered_data, ['Team', 'Segment'], 'Day', 'Total Current Value of Stock ($)')
        else:
            config_data = create_summary_dataframe(filtered_data, group_by_cols, agg_col)

        config = create_config(
            config_data_top10 if index == 6 else config_data,
            config_data_table if index in [1, 2, 3, 8, 7, 9, 5, 4, 6] else config_data,
            'Day' if index in [1, 2, 3, 9, 7, 8] else
            'Team' if index == 5 else
            'Day',
            'Total Current Value of Stock ($)',
            None,
            None,
            'Segment' if index in [7, 8, 5, 9] else 'vendor Name / plant' if index == 6 else 'Project' if index == 1 else 'Plant' if index == 2 else None
        )

        if switch_value:
            fig = get_figure(chart_type, config, "/Inventory_db", barmode_group=barmode_group)
            if index != 4:
                figures[index-1 if index < 4 else index-2] = fig
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
                {
                    'if': {
                        'filter_query': '{Gap %} contains "-"',
                        'column_id': 'Gap %'
                    },
                    'backgroundColor': 'rgba(255, 0, 0, 0.1)',  # Light red for negative percentages
                    'color': 'red'  # Text color red for negative percentages
                },
            ]

            table = create_data_table(formatted_data, '300px', style_data_conditional)
            modal_table = create_data_table(formatted_data, '500px', style_data_conditional, scrollable=True)

            logging.debug(f"Generated table for index {index}.")
            return table, modal_table

    @app.callback(
        Output({'type': f'dashboardinv-chart-options', 'index': MATCH}, 'style'),
        [Input({'type': f'dashboardinv-Graph-switch', 'index': MATCH}, 'value')]
    )
    def toggle_chart_options(switch_value):
        if switch_value:
            return {'display': 'block'}
        else:
            return {'display': 'none'}
    @app.callback(
    Output("dashboardinv-download-link", "style"),
    Output("dashboardinv-download-link", "href"),
    Input("dashboardinv-generate-report-btn", "n_clicks"),
    State({'type': "dashboardinv-card-title", 'index': ALL}, 'children'),
    prevent_initial_call=True
)
    
        
    def generate_report(n_clicks, card_titles):
         # selected_cards = ["Total Bill by Day", "Gender Analysis"]
         pdf_buffer = io.BytesIO()
         pdf = generate_pdf_report(card_titles, figures)  # Assuming this returns a PDF in bytes
         pdf_buffer.write(pdf)
         pdf_buffer.seek(0)

         # Convert the PDF to a Base64 encoded string
         pdf_base64 = base64.b64encode(pdf_buffer.read()).decode('utf-8')
         pdf_buffer.close()

         # Create the download link with the Base64 encoded PDF
         href = f"data:application/pdf;base64,{pdf_base64}"

         return {'display': 'inline-block'}, href