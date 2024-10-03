import dash
import dash_bootstrap_components as dbc
import dash_table
from dash import dcc, html, callback, Input, Output, State
import pandas as pd
from sqlalchemy import select, MetaData, Table, delete, insert
import logging
from app.db import engine, metadata
from sqlalchemy.exc import SQLAlchemyError
import base64
import io
import dash_html_components as html
# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
 
# Fetch data function
def fetch_table_data(table_name, chunk_size=10000):
    table = Table(table_name, metadata, autoload_with=engine)
    query = select(table)
    data_chunks = []
   
    logger.debug(f"Fetching data from table: {table_name}")
    with engine.connect() as connection:
        result = connection.execution_options(stream_results=True).execute(query)
        while True:
            chunk = result.fetchmany(chunk_size)
            if not chunk:
                break
            logger.debug(f"Fetched chunk of size: {len(chunk)}")
            data_chunks.append(pd.DataFrame(chunk, columns=result.keys()))
   
    data = pd.concat(data_chunks, ignore_index=True)
    logger.debug(f"Total rows fetched: {len(data)}")
    return data
 
def fetch_all_data():
    tables = ['Teams_Table', 'Segments_Table', 'Projects_Table']
    data = {}
    for table_name in tables:
        df = fetch_table_data(table_name)
        data[table_name] = df.to_dict('records')  # Convert DataFrame to list of dictionaries
    return data
 
def create_modal():
    return dbc.Modal(
        [
            dbc.ModalHeader(
                dbc.ModalTitle("Add New Entry"),
                style={'backgroundColor': '#a8dadc', 'color': '#1d3557'}
            ),
            dbc.ModalBody(
                html.Div(id="modal-form-content", className="p-3")
            ),
            dbc.ModalFooter([
                dbc.Button("Add", id="modal-submit-button", style={'backgroundColor': '#a8dadc', 'color':'#1d3557','border': 'none'}, className="mr-2"),
            ]),
        ],
        id="modal",
        is_open=False,
        centered=True,
        size="lg",
        backdrop=True,
    )
 
def create_data_designed_table(title, data):
    table_id = title.replace(' ', '-').lower()
    if data:
        columns = list(data[0].keys())  # Extract columns from the first dictionary
    else:
        columns = []
   
    logger.debug(f"Creating table for: {title} with columns {columns}")
 
    return html.Div(
        [
            html.H3(title, style={'textAlign': 'left', 'color': '#27496D'}),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Upload(
                            id=f"upload-{table_id}",
                            children=dbc.Button(
                                "Upload Excel",
                                style={'backgroundColor': '#dfffd6', 'color': '#3f784c', 'border': 'none'}  # Green button
                            ),
                            multiple=False,
                        ),
                        width="auto",  # Adjust width as needed
                        style={'display': 'inline-block', 'marginRight': '4px'}
                    ),
                    dbc.Col(
                        dcc.Upload(
                            id=f"upload-replace-{table_id}",
                            children=dbc.Button(
                                "Replace with Excel",
                                style={'backgroundColor': '#ffdddd', 'color': '#d9534f', 'border': 'none'}  # Red button
                            ),
                            multiple=False,
                        ),
                        width="auto",  # Adjust width as needed
                        style={'display': 'inline-block', 'marginRight': 'auto'}
                    ),
                    dbc.Col(
                        dbc.Button(
                            "+ New",
                            id=f"add-action-btn-{table_id}",
                            style={'backgroundColor': '#a8dadc', 'color': '#1d3557', 'float': 'right', 'border': 'none'},  # Blue button
                            className="mb-2",
                        ),
                        width="auto",  # Adjust width as needed
                        style={'display': 'inline-block', 'marginLeft': 'auto', 'marginRight': '4px'}
                    ),
                    dbc.Col(
                        dbc.Button(
                            "Save",
                            id=f"save-action-btn-{table_id}",
                            style={'backgroundColor': '#ef8a17', 'color': '#ffffff', 'float': 'right', 'border': 'none'},  # Orange button
                            className="mb-2",
                        ),
                        width="auto",  # Adjust width as needed
                        style={'display': 'inline-block', 'marginLeft': '4px'}
                    ),
                ],
                align="center",  # Center align the columns vertically
                style={'marginBottom': '10px'}
            ),
            dash_table.DataTable(
                id=f"table-{table_id}",
                columns=[{"name": i, "id": i, "editable": True} for i in columns],
                data=data,
                editable=True,
                row_deletable=True,
                filter_action="native",
                page_size=20,
                style_table={'height': 'auto' if len(data) < 20 else '400px', 'overflowY': 'auto' if len(data) >= 20 else 'visible'},
                style_cell={'textAlign': 'left'},
                style_header={'position': 'sticky', 'top': 0, 'backgroundColor': '#ffffff', 'zIndex': 1},
                virtualization=False,  # Disable virtualization to allow for dynamic height adjustment
            ),
            dcc.Store(id=f"store-{table_id}-columns", data=columns),
            dbc.Alert(id=f"notification-alert-{table_id}", is_open=False, dismissable=True, duration=4000),
            dcc.ConfirmDialog(id=f"confirm-dialog-{table_id}")
        ],
        style={'padding': '20px', 'backgroundColor': '#ffffff', 'borderRadius': '10px'}
    )
 
 
def layout():
    # Fetch data every time layout is generated
    all_data = fetch_all_data()
 
    return dbc.Container(
        fluid=True,
        children=[
            dbc.Row(
                [
                    dbc.Col(create_data_designed_table("Teams Table", all_data['Teams_Table']), width=6),
                    dbc.Col(create_data_designed_table("Segments Table", all_data['Segments_Table']), width=6),
                ], className="m-3"
            ),
            dbc.Row(
                [
                    dbc.Col(create_data_designed_table("Projects Table", all_data['Projects_Table']), width=12)
                ],
                className="m-3"
            ),
            create_modal()  # Include the modal in the layout
        ]
    )
 
def validate_data(data, required_columns):
    for row in data:
        for column in required_columns:
            if row.get(column) in [None, '', []]:
                logger.debug(f"Missing value for column: {column} in row: {row}")
                return False, f"Missing value for column: {column}"
    return True, "Data is valid"
 
def update_table(table_name, rows):
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
   
    # Define required columns for each table
    required_columns = {
        "Teams_Table": ['MRP ctrlr', 'Team'],
        "Segments_Table": ['Material-Plant', 'Segment'],
        "Projects_Table": ['Material Number', 'Project']
    }
   
    # Validate data
    is_valid, message = validate_data(rows, required_columns[table_name])
    if not is_valid:
        logger.error(f"Validation error: {message}")
        raise ValueError(message)
   
    try:
        with engine.begin() as connection:
            connection.execute(delete(table))  # Delete existing rows
            connection.execute(insert(table), rows)  # Insert new rows
    except SQLAlchemyError as e:
        logger.error(f"Error updating {table_name} table: {e}")
        raise
 
# Registering callbacks
def register_callbacks(app):
    @app.callback(
        Output("modal", "is_open"),
        Output("modal-form-content", "children"),
        [Input(f"add-action-btn-teams-table", "n_clicks"),
         Input(f"add-action-btn-segments-table", "n_clicks"),
         Input(f"add-action-btn-projects-table", "n_clicks")],
        [State("modal", "is_open")]
    )
    def toggle_modal(teams_click, segments_click, projects_click, is_open):
        ctx = dash.callback_context
        if not ctx.triggered:
            return is_open, ""
       
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
       
        if button_id == "add-action-btn-teams-table":
            form_content = create_form("Teams")
        elif button_id == "add-action-btn-segments-table":
            form_content = create_form("Segments")
        elif button_id == "add-action-btn-projects-table":
            form_content = create_form("Projects")
        else:
            form_content = ""
       
        return not is_open, form_content
 
    @app.callback(
        [Output(f"confirm-dialog-teams-table", "displayed"),
         Output(f"confirm-dialog-segments-table", "displayed"),
         
         Output(f"confirm-dialog-projects-table", "displayed")],
        [Input(f"save-action-btn-teams-table", "n_clicks"),
         Input(f"save-action-btn-segments-table", "n_clicks"),
        
         Input(f"save-action-btn-projects-table", "n_clicks")]
    )
    def display_confirm_dialog(n_clicks_teams, n_clicks_segments, n_clicks_projects):
        ctx = dash.callback_context
        if not ctx.triggered:
            return [False, False, False]
       
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
       
        if button_id == "save-action-btn-teams-table":
            return [True, False, False]
        elif button_id == "save-action-btn-segments-table":
            return [False, True, False]
        elif button_id == "save-action-btn-projects-table":
            return [False, False, True]
        return [False, False, False]
 
    @app.callback(
        [Output("table-teams-table", "data"),
         Output("table-segments-table", "data"),
        
         Output("table-projects-table", "data"),
         Output("notification-alert-teams-table", "is_open"),
         Output("notification-alert-teams-table", "children"),
         Output("notification-alert-teams-table", "color"),
         Output("notification-alert-segments-table", "is_open"),
         Output("notification-alert-segments-table", "children"),
         Output("notification-alert-segments-table", "color"),
         Output("notification-alert-projects-table", "is_open"),
         Output("notification-alert-projects-table", "children"),
         Output("notification-alert-projects-table", "color")],
        [Input("modal-submit-button", "n_clicks"),
         Input("confirm-dialog-teams-table", "submit_n_clicks"),
         Input("confirm-dialog-segments-table", "submit_n_clicks"),
         
         Input("confirm-dialog-projects-table", "submit_n_clicks"),
         Input("upload-teams-table", "contents"),
         Input("upload-replace-teams-table", "contents"),
         Input("upload-segments-table", "contents"),
         Input("upload-replace-segments-table", "contents"),
         Input("upload-projects-table", "contents"),
         Input("upload-replace-projects-table", "contents")],
        [State("modal", "is_open"),
         State("modal-form-content", "children"),
         State("table-teams-table", "data"),
         State("table-segments-table", "data"),
         
         State("table-projects-table", "data"),
         State("upload-teams-table", "filename"),
         State("upload-replace-teams-table", "filename"),
         State("upload-segments-table", "filename"),
         State("upload-replace-segments-table", "filename"),
         State("upload-projects-table", "filename"),
         State("upload-replace-projects-table", "filename")]
    )
    def handle_table_actions(
        n_clicks_submit, n_clicks_confirm_teams, n_clicks_confirm_segments,
         n_clicks_confirm_projects,
        upload_teams, upload_replace_teams, upload_segments, upload_replace_segments,
         upload_projects, upload_replace_projects,
        is_open, form_content, teams_data, segments_data, projects_data,
        upload_teams_filename, upload_replace_teams_filename, upload_segments_filename, upload_replace_segments_filename,
         upload_projects_filename, upload_replace_projects_filename
    ):
        ctx = dash.callback_context
        if not ctx.triggered:
            return (teams_data, segments_data, projects_data, False, "", "", False, "", "", False, "", "")
       
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        alert_outputs = (False, "", "", False, "", "", False, "", "")
 
        # Handle confirmation dialog responses
        if button_id == "confirm-dialog-teams-table":
            try:
                update_table("Teams_Table", teams_data)
                # Fetch updated data
                all_data = fetch_all_data()
                alert_outputs = (True, "Teams table updated successfully!", "success") + alert_outputs[3:]
                return (all_data['Teams_Table'], all_data['Segments_Table'],  all_data['Projects_Table']) + alert_outputs
            except Exception as e:
                logger.error(f"Error updating Teams table: {e}")
                return (teams_data, segments_data, projects_data, True, f"Error updating Teams table: {e}", "danger") + alert_outputs[3:]
        elif button_id == "confirm-dialog-segments-table":
            try:
                update_table("Segments_Table", segments_data)
                # Fetch updated data
                all_data = fetch_all_data()
                alert_outputs = alert_outputs[:3] + (True, "Segments table updated successfully!", "success") + alert_outputs[6:]
                return (all_data['Teams_Table'], all_data['Segments_Table'],  all_data['Projects_Table']) + alert_outputs
            except Exception as e:
                logger.error(f"Error updating Segments table: {e}")
                return (teams_data, segments_data, projects_data) + alert_outputs[:3] + (True, f"Error updating Segments table: {e}", "danger") + alert_outputs[6:]
        
        elif button_id == "confirm-dialog-projects-table":
            try:
                update_table("Projects_Table", projects_data)
                # Fetch updated data
                all_data = fetch_all_data()
                alert_outputs = alert_outputs[:9] + (True, "Projects table updated successfully!", "success")
                return (all_data['Teams_Table'], all_data['Segments_Table'], all_data['Projects_Table']) + alert_outputs
            except Exception as e:
                logger.error(f"Error updating Projects table: {e}")
                return (teams_data, segments_data, projects_data) + alert_outputs[:9] + (True, f"Error updating Projects table: {e}", "danger")
       
        # Handle file upload
        if button_id in ["upload-teams-table", "upload-replace-teams-table", "upload-segments-table", "upload-replace-segments-table", "upload-projects-table", "upload-replace-projects-table"]:
            upload_contents = ctx.triggered[0]['value']
            upload_filename = None
            if button_id == "upload-teams-table":
                upload_filename = upload_teams_filename
            elif button_id == "upload-replace-teams-table":
                upload_filename = upload_replace_teams_filename
            elif button_id == "upload-segments-table":
                upload_filename = upload_segments_filename
            elif button_id == "upload-replace-segments-table":
                upload_filename = upload_replace_segments_filename
            elif button_id == "upload-projects-table":
                upload_filename = upload_projects_filename
            elif button_id == "upload-replace-projects-table":
                upload_filename = upload_replace_projects_filename
           
            if upload_contents and upload_filename:
                content_type, content_string = upload_contents.split(',')
                decoded = base64.b64decode(content_string)
                try:
                    if 'csv' in upload_filename:
                        df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
                    elif 'xls' in upload_filename:
                        df = pd.read_excel(io.BytesIO(decoded))
                   
                    new_data = df.to_dict('records')
                   
                    if button_id == "upload-teams-table":
                        teams_data.extend(new_data)
                        alert_outputs = (True, "File uploaded and data appended successfully!", "success") + alert_outputs[3:]
                    elif button_id == "upload-replace-teams-table":
                        teams_data = new_data
                        alert_outputs = (True, "File uploaded and data replaced successfully!", "success") + alert_outputs[3:]
                    elif button_id == "upload-segments-table":
                        segments_data.extend(new_data)
                        alert_outputs = alert_outputs[:3] + (True, "File uploaded and data appended successfully!", "success") + alert_outputs[6:]
                    elif button_id == "upload-replace-segments-table":
                        segments_data = new_data
                        alert_outputs = alert_outputs[:3] + (True, "File uploaded and data replaced successfully!", "success") + alert_outputs[6:]
                   
                   
                    elif button_id == "upload-projects-table":
                        projects_data.extend(new_data)
                        alert_outputs = alert_outputs[:9] + (True, "File uploaded and data appended successfully!", "success")
                    elif button_id == "upload-replace-projects-table":
                        projects_data = new_data
                        alert_outputs = alert_outputs[:9] + (True, "File uploaded and data replaced successfully!", "success")
                   
                    return (teams_data, segments_data, projects_data) + alert_outputs
                except Exception as e:
                    logger.error(f"Error processing file: {e}")
                    error_message = f"Error processing file: {e}"
                    if button_id == "upload-teams-table":
                        alert_outputs = (True, error_message, "danger") + alert_outputs[3:]
                    elif button_id == "upload-replace-teams-table":
                        alert_outputs = (True, error_message, "danger") + alert_outputs[3:]
                    elif button_id == "upload-segments-table":
                        alert_outputs = alert_outputs[:3] + (True, error_message, "danger") + alert_outputs[6:]
                    elif button_id == "upload-replace-segments-table":
                        alert_outputs = alert_outputs[:3] + (True, error_message, "danger") + alert_outputs[6:]
                    
                    elif button_id == "upload-projects-table":
                        alert_outputs = alert_outputs[:9] + (True, error_message, "danger")
                    elif button_id == "upload-replace-projects-table":
                        alert_outputs = alert_outputs[:9] + (True, error_message, "danger")
                   
                    return (teams_data, segments_data, projects_data) + alert_outputs
       
        # Handle adding new rows
        if button_id == "modal-submit-button":
            if not isinstance(form_content, list):
                form_content = form_content['props']['children']
            new_row = extract_form_data(form_content)
            form_id = form_content[0]['props']['id'].split('-')[1]
           
            # Add debugging to check new row values
            logger.debug(f"New row data: {new_row}")
           
            # Adjust keys to match form field IDs
            new_row = {k.replace('MRP_ctrlr', 'MRP ctrlr').replace('Team', 'Team').replace('Material_Plant', 'Material-Plant').replace('Segment', 'Segment').replace('Material', 'Material').replace('SL_1249', 'SL 1249').replace('SL_1002', 'SL 1002').replace('Trade_Interco', 'trade/interco').replace('Vendor_Name_Plant', 'vendor Name / plant').replace('Country', 'country').replace('Local_Europe_Overseas', 'Local/europe/Overseas').replace('Planning_Location', 'planning location').replace('Country_Name', 'country name').replace('Material_Number', 'Material Number').replace('Project', 'Project'): v for k, v in new_row.items()}
           
            # Validate new row data
            if form_id == "teams":
                required_columns = ['MRP ctrlr', 'Team']
                is_valid, message = validate_data([new_row], required_columns)
                if not is_valid:
                    logger.debug(f"Validation failed: {message}")
                    return (teams_data, segments_data, projects_data, True, message, "danger") + alert_outputs[3:]
                teams_data.append(new_row)
                success_message = "Team added successfully!"
                alert_outputs = (True, success_message, "success") + alert_outputs[3:]
            elif form_id == "segments":
                required_columns = ['Material-Plant', 'Segment']
                is_valid, message = validate_data([new_row], required_columns)
                if not is_valid:
                    logger.debug(f"Validation failed: {message}")
                    return (teams_data, segments_data, projects_data) + alert_outputs[:3] + (True, message, "danger") + alert_outputs[6:]
                segments_data.append(new_row)
                success_message = "Segment added successfully!"
                alert_outputs = alert_outputs[:3] + (True, success_message, "success") + alert_outputs[6:]
            
            elif form_id == "projects":
                required_columns = ['Material Number', 'Project']
                is_valid, message = validate_data([new_row], required_columns)
                if not is_valid:
                    logger.debug(f"Validation failed: {message}")
                    return (teams_data, segments_data, projects_data) + alert_outputs[:9] + (True, message, "danger")
                projects_data.append(new_row)
                success_message = "Project added successfully!"
                alert_outputs = alert_outputs[:9] + (True, success_message, "success")
            else:
                return (teams_data, segments_data, projects_data, True, "Failed to add data.", "danger") + alert_outputs[3:]
           
            return (teams_data, segments_data, projects_data) + alert_outputs
       
        return (teams_data, segments_data, projects_data) + alert_outputs
 
def create_form(table_name):
    form_id = f"form-{table_name.lower().replace(' ', '-')}"
    form_fields = []
   
    if table_name == "Teams":
        form_fields = [
            dcc.Input(id=f"{form_id}-MRP_ctrlr", type="text", placeholder="MRP Controller"),
            dcc.Input(id=f"{form_id}-Team", type="text", placeholder="Team"),
        ]
    elif table_name == "Segments":
        form_fields = [
            dcc.Input(id=f"{form_id}-Material_Plant", type="text", placeholder="Material-Plant"),
            dcc.Input(id=f"{form_id}-Segment", type="text", placeholder="Segment"),
        ]
    
    elif table_name == "Projects":
        form_fields = [
            dcc.Input(id=f"{form_id}-Material_Number", type="text", placeholder="Material Number"),
            dcc.Input(id=f"{form_id}-Project", type="text", placeholder="Project"),
        ]
   
    return html.Div(
        id=form_id,
        children=form_fields
    )
 
def extract_form_data(form_content):
    form_data = {}
    for field in form_content:
        if 'props' in field:
            field_id = field['props']['id'].split('-')[-1]
            field_value = field['props'].get('value', '')  # Use get to avoid KeyError
            form_data[field_id] = field_value
            # Add debugging to check values being extracted
            logger.debug(f"Extracted field {field_id} with value {field_value}")
    return form_data