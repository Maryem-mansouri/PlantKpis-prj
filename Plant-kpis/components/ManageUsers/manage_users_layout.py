import dash
from dash.dependencies import Input, Output, State, ALL
import dash_html_components as html
import dash_table
import dash_bootstrap_components as dbc
import dash_core_components as dcc
from flask_sqlalchemy import SQLAlchemy
from components.ManageUsers.accessUsers import get_users_in_same_department_but_not_current, update_user_status, delete_user_from_database
 
def manage_users_layout():
    df = get_users_in_same_department_but_not_current()
    print(df)
    df = df[['User_id', 'Email', 'Business Unit', 'Status', 'Role Name']]
   
    if df.empty:
        return html.Div("No users found in the same department.")
 
    # Map True/False to display Active/Suspended
    df['Status'] = df['Status'].map({True: '🟢 Active', False: '🔴 Suspended'})
 
    # Define available roles for the Role Name dropdown
    role_options = [
        {'label': 'Admin', 'value': 'Admin'},
        {'label': 'User', 'value': 'User'},
        # Add more roles as necessary
    ]
 
    return dbc.Container(
        [
            dcc.Store(id='selected-user-id', data=None),
            dbc.Card(
                [
                    dbc.CardHeader(
                        dbc.Row(
                            [
                                dbc.Col(
                                    html.H3("Manage Users", className="card-title text-white my-auto"),
                                    width="auto"
                                ),
                                dbc.Col(
                                    dbc.ButtonGroup(
                                        [
                                            dbc.Button(
                                                children=[
                                                    html.I(className="bi bi-check-circle me-2"),
                                                    "Save Changes"
                                                ],
                                                id='save-button1', n_clicks=0, color="success", className="me-2 my-auto",
                                                style={'fontWeight': 'bold', 'borderRadius': '10px', 'fontSize': '12px', 'padding': '5px 10px'}
                                            ),
                                            dbc.Button(
                                                children=[
                                                    html.I(className="bi bi-trash me-2"),
                                                    "Delete Selected User"
                                                ],
                                                id="delete-button", n_clicks=0, color="danger", className="my-auto",
                                                style={'fontWeight': 'bold', 'borderRadius': '10px', 'fontSize': '12px', 'padding': '5px 10px'}
                                            ),
                                        ],
                                        className="d-flex justify-content-end align-items-center"
                                    ),
                                    width="auto"
                                ),
                            ],
                            justify="between",
                            align="center"
                        ),
                        style={"backgroundColor": "#fb8500"},
                    ),
 
                    dbc.CardBody(
                        [
                            dash_table.DataTable(
                                id='user-table',
                                columns=[
                                    {"name": 'User ID', "id": 'User_id', "editable": False},
                                    {"name": 'Email', "id": 'Email', "editable": False},
                                    {"name": 'Business Unit', "id": 'Business Unit', "editable": False},
                                    {"name": 'Status', "id": 'Status', "presentation": "dropdown", "editable": True},
                                    {"name": 'Role Name', "id": 'Role Name', "presentation": "dropdown", "editable": True},
                                ],
                                data=df.to_dict('records'),
                                editable=True,
                                row_selectable="single",
                                dropdown={
                                    'Status': {
                                        'options': [
                                            {'label': '🟢 Active', 'value': '🟢 Active'},
                                            {'label': '🔴 Suspended', 'value': '🔴 Suspended'},
                                        ]
                                    },
                                    'Role Name': {
                                        'options': role_options
                                    }
                                },
                                style_table={'overflowX': 'auto'},
                                style_cell={
                                    'textAlign': 'left',
                                    'padding': '10px',
                                    'font-size': '14px',
                                    'border': '1px solid #e0e0e0',
                                    'height': 'auto',
                                    'minWidth': '100px', 'width': '150px', 'maxWidth': '180px',
                                    'whiteSpace': 'normal'
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
                                ],
                                page_current=0,
                                page_size=5,
                                page_action='native',
                                style_as_list_view=True,
                                markdown_options={"html": True}
                            ),
                            # Confirm Dialog
                            dcc.ConfirmDialog(
                                id='confirm-delete',
                                message="Voulez-vous vraiment supprimer cet utilisateur avec l'ID utilisateur ?",
                            ),
                            dbc.Alert(
                                id='save-alert',
                                is_open=False,
                                duration=5000,
                                style={
                                    'backgroundColor': 'rgb(173,216,228,0.88)',  # Light blue background
                                    'color': 'white',  # White text
                                    'borderRadius': '10px',  # Rounded corners
                                    'textAlign': 'center',  # Centered text
                                    'border': '0',
                                    'fontWeight': 'bold',
                                    'width': 'fit-content',  # Fit content width
                                    'position': 'fixed',  # Fixed position
                                    'top': '20px',  # Position at the top of the page
                                    'right': '20px',  # Position on the right side of the page
                                    'zIndex': 9999,  # High z-index to appear in front of other elements
                                },
                                dismissable=True,
                                className="custom-alert"
                            ),
                        ]
                    ),
                ],
                className="mt-4 shadow-sm",
                style={"border": "none"},
            ),
        ],
        fluid=True,
    )
def register_callbacks(app):
    @app.callback(
        [Output('confirm-delete', 'displayed'),
         Output('selected-user-id', 'data')],
        [Input('delete-button', 'n_clicks')],
        [State('user-table', 'selected_rows'),
         State('user-table', 'data')]
    )
    def confirm_deletion(delete_n_clicks, selected_rows, rows):
        if delete_n_clicks > 0 and selected_rows is not None and len(selected_rows) > 0:
            selected_user_id = rows[selected_rows[0]]['User_id']
            return True, selected_user_id  # Display the dialog and pass the user ID
        return False, None  # Do not display the dialog
 
    @app.callback(
        [Output('user-table', 'data'),
         Output('save-alert', 'children'),
         Output('save-alert', 'color'),
         Output('save-alert', 'is_open')],
        [Input('confirm-delete', 'submit_n_clicks'),
         Input('save-button1', 'n_clicks')],
        [State('selected-user-id', 'data'),
         State('user-table', 'data')]
    )
    def manage_users(confirm_clicks, save_clicks, selected_user_id, rows):
        ctx = dash.callback_context
 
        if not ctx.triggered:
            return rows, "", "info", False
 
        triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
 
        if triggered_id == 'save-button1':
            try:
                for row in rows:
                    status = True if row['Status'] == '🟢 Active' else False
                    role_name = row['Role Name']  # Get the role name from the table
                    update_message, error = update_user_status(row['User_id'], status, role_name)
                    if error:
                        return rows, update_message, "danger", True
                return rows, "Changes saved successfully.", "success", True
            except Exception as e:
                return rows, f"An error occurred: {str(e)}", "danger", True
 
        elif triggered_id == 'confirm-delete':
            if confirm_clicks and selected_user_id:
                try:
                    # Use the delete_user_from_database function
                    message, error = delete_user_from_database(selected_user_id)
                    if not error:
                        # Remove the deleted user from the rows list
                        rows = [row for row in rows if row['User_id'] != selected_user_id]
                        return rows, message, "success", True
                    else:
                        return rows, message, "danger", True
                except Exception as e:
                    return rows, f"An error occurred: {str(e)}", "danger", True
 
        return rows, "", "info", False