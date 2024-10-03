# layout.py
import dash_html_components as html
import dash_core_components as dcc
from components import navbar ,sidebar
from pages import Inventory_db,reduced_db_manip,EDI_SAP,Overall_dashboard ,reduced_db_manip,Clusters
from components.ManageUsers.manage_users_layout import manage_users_layout
from components.ManageUsers.currentUser import get_logged_in_user_info
def create_layout():
    return html.Div([
        dcc.Location(id='url', refresh=False),
        html.Div(
            id='navbar-container',
            children=navbar.create_navbar()
        ),
        html.Div(
            id='page-content',
            style={
                "margin-left": "0",  # No margin left to avoid pushing the content
                "position": "relative",
                "z-index": 1,  # Ensure content appears below the sidebar
            },
        ),
        sidebar.sidebar,
    ])

def display_page(pathname):
    
    user_id, email, department, role = get_logged_in_user_info()
    if pathname == '/Inventory_db':
        return Inventory_db.layout()
    elif pathname == '/EDI_SAP':
        return EDI_SAP.layout()
    elif pathname == '/reduced_db_manip':
        return reduced_db_manip.layout()
    elif pathname == '/Clusters':
        return Clusters.layout()
    #elif pathname == '/EDI_Cloud_db':
    #    return EDI_Cloud_db.layout()
    elif pathname == '/Overall_dashboard':
        return Overall_dashboard.layout()
    elif pathname == '/ManageUsers':
        if role == 'Admin':  # Only allow access if the user is an Admin
            return manage_users_layout()
        else:
            return html.Div([
                html.H1("Access Denied"),
                html.P("You do not have permission to access this page.")
            ])
    else:
        return Overall_dashboard.layout()  # Default to Inventory3 layout
