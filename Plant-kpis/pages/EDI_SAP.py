import dash
import re
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
from data.EDI_SAP import (get_next_six_months_from_filter, create_customer_demand_pivot,engine,fetch_data,fetch_filter_options,filter_ship_month, filter_ship_date, filter_week, filter_plant, filter_year, create_summary_dataframe, create_pivot_table, create_pivot_table_with_columns)
from config.config import (create_config, format_value)
import logging
import base64
import io
from data.ETL.ETL_singl_file_EDI import process_file,insert_data_to_db

# Setup logging
logging.basicConfig(level=logging.INFO)

# Define the function to get the current year
def get_current_year():
    return datetime.now().year
next_six_months = get_next_six_months_from_filter(filter_ship_month)
# Define the layout function
def layout():
    filter_plant, filter_year, filter_week, filter_ship_month, filter_ship_date = fetch_filter_options()
    current_year = get_current_year()
    return dbc.Container(
        fluid=True,
        children=[
            dcc.Store(id='available-year'),
            dcc.Store(id='available-weeks-edi4'),
            dcc.Store(id='dashboard7-filters'),
            dcc.Store(id='upload-process-status', data="idle"),

            # File processing modal
            dbc.Modal(
                [
                    dbc.ModalHeader(dbc.ModalTitle("Processing File")),
                    dbc.ModalBody(html.Div("Your file is being processed, please wait...")),
                ],
                id="processing-modal",
                is_open=False,
                centered=True,
            ),

            # Success modal
            dbc.Modal(
    [
        dbc.ModalBody(
                    [
                        html.Div(
                            html.Img(src="assets/check.png", style={"width": "50px", "height": "50px"}), 
                            style={"text-align": "center"}
                        ),
                        html.Div(
                            "File processed and inserted successfully!", 
                            style={"text-align": "center", "margin-top": "10px", "font-size": "18px"}
                        )
                    ]
                ),
                ],
                id="success-modal",
                is_open=False,
                centered=True,
                backdrop=True,
                size="sm",  # Optional: adjusts the modal size
            ),

            dbc.Row(
                [
                        dbc.Col(
        [
            # dcc.Upload component
            dcc.Upload(
                id='upload-data',
                children=dbc.Button(
                    children=[
                        "Upload Data",
                        html.Img(src="assets/solar_export-outline.png", className='ms-2', height="20px")
                    ],
                    className="me-2",
                    id="upload-data-btn",
                    style={'fontWeight': 'bold', 'backgroundColor': '#4d935c', 'borderColor': '#4d935c', 'borderRadius': '10px'}
                ),
                multiple=False,
                style={'display': 'inline-block'}
            ),
            # Wrap html.Div in children list
            html.Div(id='upload-status', className="mt-2", style={"font-size": "12px", "color": "#333", "display": "inline-block", "margin-left": "10px"})
        ],
        width="auto",
        className="d-flex align-items-center"
    ),
                        
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
                            create_filter_dropdown("Plant", 'filter-location', filter_plant),
                            create_filter_dropdown("Year", 'filter-year', filter_year, [current_year]),
                            create_filter_dropdown("Week", 'filter-week-edi', filter_week),
                            
                            create_filter_dropdown("Ship Month", 'filter-ship-month', filter_ship_month,next_six_months),
                            create_filter_dropdown("Ship date", 'filter-ship-date', filter_ship_date),
                        ],
                        className="d-flex justify-content-end align-items-center"
                    )
                ],
                className="m-3"
            ),
            dbc.Row(
                [
                    dbc.Col(create_card("EDI variation Week Over Week", 1, 'dashboard7', True, 'Line'), width=6, className="p-1"),
                    dbc.Col(create_card("EDI variation by Client", 4, 'dashboard7', False, 'Bar'), width=6, className="p-1"),
                ],
                
                className="m-1"
            ),
            dbc.Row(
                [
                    dbc.Col(create_card("demande $ by Project", 2, 'dashboard7', True, 'Bar'), width=6, className="p-1"),
                    dbc.Col(create_card("demande $ by Team", 3, 'dashboard7', True, 'Bar'), width=6, className="p-1"),
                ],
                className="m-1"
            
            ),
              dbc.Row(
                [
                    dbc.Col(create_card("demande variation $ by client ", 5, 'dashboard7', False, 'Bar'), width=12, className="p-1"),
                  
                ],
                className="m-1")
        ]
    )

# Register callbacks
def register_callbacks(app):
    @app.callback(
    Output('available-weeks-edi4', 'data'),
    [Input('filter-year', 'value')]
)
    def update_week_based_on_year(selected_years):
        if not selected_years:
            selected_years = [get_current_year()]
        
        # Fetch data based on the selected years
        filtered_df_EDI = fetch_data(
            plant=filter_plant, 
            year=selected_years, 
            week=filter_week, 
            ship_date=filter_ship_date, 
            ship_month=filter_ship_month
        )
        
        weeks_EDI = filtered_df_EDI['Week Number'].unique().tolist()
        weeks_combined = sorted(list(set(weeks_EDI)))
        
        return weeks_combined

    @app.callback(
        Output('filter-week-edi', 'options'),
        [Input('available-weeks-edi4', 'data')]
    )
    def update_week_dropdown(available_weeks):
        if available_weeks is None:
            return []
        sorted_weeks = sorted(available_weeks, key=lambda x: int(x))
        return [{'label': week, 'value': week} for week in sorted_weeks]

    @app.callback(
    Output('filter-ship-date', 'options'),
    [Input('filter-ship-month', 'value')]
)
    def update_ship_date_based_on_month(selected_months):
        if not selected_months:
            return []
        
        # Fetch data based on the selected months
        filtered_df_ship = fetch_data(
            plant=filter_plant,
            year=filter_year,
            week=filter_week,
            ship_date=filter_ship_date,
            ship_month=selected_months
        )
        
        ship_dates = sorted(filtered_df_ship['Cust Req S'].unique().tolist())
        
        return [{'label': date, 'value': date} for date in ship_dates]
        
    @app.callback(
        Output('dashboard7-filters', 'data'),
        [
            Input('filter-location', 'value'),
            Input('filter-year', 'value'),
            Input('filter-week-edi', 'value'),
            Input('filter-ship-date', 'value'),
            Input('filter-ship-month', 'value')
        ]
    )
    def update_filters(loc, year, week, ship_date, ship_month):
        start_time = time.time()
        if not year:
            year = [datetime.now().year]
        if not loc:
            loc = ['1002', '1249']
        if not week:
            week = filter_week
        if not ship_date:
            ship_date = filter_ship_date
        if not ship_month:
            ship_month = filter_ship_month
            
        filtered_data = fetch_data(loc, year, week, ship_date, ship_month)
        end_time = time.time()
        logging.info(f"Filter update took {end_time - start_time:.2f} seconds")
        logging.info(f"Filtered Data: {filtered_data.head()}")
        return filtered_data.to_dict('records')

    
    @app.callback(
        [
            Output({'type': 'dashboard7-dynamic-content', 'index': MATCH}, 'children'),
            Output({'type': 'dashboard7-dynamic-content-modal', 'index': MATCH}, 'children'),
        ],
        [
            Input({'type': 'dashboard7-Graph-switch', 'index': MATCH}, 'value'),
            Input({'type': 'dashboard7-chart-type', 'index': MATCH}, 'value'),
            Input('dashboard7-filters', 'data'),
            Input('filter-week-edi', 'value'),
        ],
        [State({'type': 'dashboard7-dynamic-content', 'index': MATCH}, 'id')]
    )
    def update_charts(switch_value, chart_type, filters, selected_weeks, component_id):
        start_time = time.time()
        if not filters:
            return dash.no_update, dash.no_update
    
        filtered_df_EDI = pd.DataFrame(filters)
        logging.info(f"Filtered DataFrame: {filtered_df_EDI.head()}")  # Log the filtered DataFrame
        if filtered_df_EDI.empty:
            return "No data available for the selected filters.", dash.no_update
            
        index = component_id['index']
    
        if index == 1:
            group_by_cols = ['Week Number']
            agg_col = 'Bklg Value'
        elif index == 2:
            group_by_cols = ['Project', 'Week Number']
            agg_col = 'Bklg Value'
        elif index == 3:
            group_by_cols = ['Team', 'Segment']
            agg_col = 'Bklg Value'
        elif index == 4:
            group_by_cols = ['Soldto Name','Week Number']
            agg_col = 'Bklg Value'
        elif index == 5:
            group_by_cols = ['Soldto Name','Week Number']
            agg_col = 'Bklg Value'
           
        else:
            return dash.no_update, dash.no_update
    
        if index == 1:
            config_data = create_summary_dataframe(filtered_df_EDI, group_by_cols, agg_col)
            config_data_table = create_pivot_table_with_columns(filtered_df_EDI, 'Week Number', 'Bklg Value')
        if index == 2:
            config_data = create_summary_dataframe(filtered_df_EDI, group_by_cols, agg_col)
            config_data_table = create_pivot_table(filtered_df_EDI, 'Project','Week Number', 'Bklg Value')
        if index == 3:
            config_data = create_summary_dataframe(filtered_df_EDI, group_by_cols, agg_col)
            config_data_table = create_pivot_table(filtered_df_EDI,['Team', 'Segment'], 'Week Number', 'Bklg Value')
        if index == 4:
            config_data = create_summary_dataframe(filtered_df_EDI, group_by_cols, agg_col)
            config_data_table = create_pivot_table(filtered_df_EDI,'Soldto Name', 'Week Number', 'Bklg Value')
        if index == 5:
            config_data = create_summary_dataframe(filtered_df_EDI, group_by_cols, agg_col)
            config_data_table = create_customer_demand_pivot(filtered_df_EDI,'Soldto Name', 'Week Number', 'Bklg Value','Document','Material Nbr','Cust Req S')
        else:
            config_data = create_summary_dataframe(filtered_df_EDI, group_by_cols, agg_col)
    
        config = create_config(
            config_data  if index in [1, 2, 3,4,5]else None,
            config_data_table if index in [1, 2, 3,4,5]else None,
            'Week Number' if index in [1,2,4,5] else 
            'Team' if index == 3 else None,
            'Bklg Value' if index in [1,2,3,4,5] else None,
            None,
            None,
            'Segment' if index == 3 else 'Project' if index == 2 else 'Soldto Name'  if index == 4 else None
        )
    
        logging.info(f"Config Data: {config_data.head()}")  # Log the config data
        logging.info(f"Config Data Table: {config_data_table.head()}")  # Log the pivot table data
    
        if switch_value:
            fig = card.get_figure(chart_type, config, "/EDI")
            if fig is None:
                logging.error("Figure generation failed.")
            end_time = time.time()
            logging.info(f"Chart update took {end_time - start_time:.2f} seconds")
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
    
            table = dcc.Loading(
                children=[dash_table.DataTable(
                    data=formatted_data.to_dict('records'),
                    columns=[{'name': str(i), 'id': str(i)} for i in formatted_data.columns],
                    style_table={'height': '300px', 'overflowY': 'auto'},
                    style_cell={
                        'textAlign': 'left',
                        'padding': '10px',
                        'font-size': '14px',
                        'border': '1px solid #e0e0e0'
                    },
                    style_header={
                        'backgroundColor': '#F1F4F9',
                        'fontWeight': 'bold',
                        'font-size': '14px',
                        'border': 'none'
                    },
                    style_data={
                        'border': 'none',
                        'font-size': '14px',
                        'color': 'black',
                        'borderBottom': '1px solid #F6F6F6'
                    },
                    style_filter={
                        'border': 'none',
                        'font-size': '14px',
                        'color': 'black',
                        'borderBottom': '1px solid #e0e0e0'
                    },
                    style_data_conditional=[
                        {
                            'if': {'row_index': 'odd'},
                            'backgroundColor': '#f9f9f9'
                        },
                        {
                            'if': {'row_index': 'even'},
                            'backgroundColor': 'white'
                        },
                        {
                        'if': {
                            'filter_query': '{Gap %} contains "-"',
                            'column_id': 'Gap %'
                        },
                        'backgroundColor': 'rgba(255, 0, 0, 0.1)',  # Light red for negative percentages
                        'color': 'red'  # Text color red for negative percentages
                },
                    ],
                    editable=True,
                    filter_action="native",
                    sort_action="native",
                    sort_mode='multi',
                    page_action='native',
                    page_current=0,
                    page_size=10,
                )], type="default"
            )
            end_time = time.time()
            logging.info(f"Table update took {end_time - start_time:.2f} seconds")
            return table, table

    @app.callback(
        Output({'type': f'dashboard4-chart-options', 'index': MATCH}, 'style'),
        [Input({'type': f'dashboard4-Graph-switch', 'index': MATCH}, 'value')]
    )
    def toggle_chart_options(switch_value):
        if switch_value:
            return {'display': 'block'}
        else:
            return {'display': 'none'}

    @app.callback(
        Output('upload-status', 'children'),
        Output('upload-process-status', 'data'),
        Output('processing-modal', 'is_open'),
        Output('success-modal', 'is_open'),
        Input('upload-data', 'contents'),
        State('upload-data', 'filename'),
        prevent_initial_call=True,
    )
    def handle_file_upload(contents, filename):
        if contents is None:
            return "No file uploaded yet.", "idle", False, False
        
        # Start processing: Open the processing modal
        status_message = process_uploaded_file(contents, filename)
        if "successfully" in status_message:
            return status_message, "success", False, True  # Close processing modal, open success modal
        else:
            return status_message, "idle", False, False  # Close processing modal, do not open success modal
    
    def process_uploaded_file(contents, filename):
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        
        try:
            if 'xlsx' in filename:
                df = pd.read_excel(io.BytesIO(decoded))
            else:
                return "Unsupported file format. Please upload an Excel file."
            
            # Use your existing ETL processing function, now passing the filename
            df_processed = process_file(df, file_name=filename)
            insert_data_to_db(df_processed)
    
            return f"File '{filename}' successfully uploaded and processed."
        
        except Exception as e:
            logging.error(f"Error processing file: {e}")
            return f"An error occurred while processing the file: {str(e)}"