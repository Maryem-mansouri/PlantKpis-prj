# callbacks.py
from dash import Input, Output, dcc ,MATCH,State
from app import app, layout
from pages import   Inventory_db , Clusters , reduced_db_manip,EDI_SAP,Overall_dashboard,reduced_db_manip
from components.ManageUsers import manage_users_layout
from dash.exceptions import PreventUpdate
# List of dashboards
dashboards = ['Inventory_db','EDI_SAP','Clusters','Overall_dashboard','reduced_db_manip','manage_users_layout']

@app.callback(
    [Output('page-content', 'children'),
     Output('navbar-container', 'className')],
    [Input('url', 'pathname')]
)
def display_page(pathname):
    # Handle the case where the URL is not recognized or is the root
    if pathname is None or pathname == '/':
        pathname = '/Overall_dashboard'  # Default to Overall_dashboard

    # Determine if the navbar should be hidden
    if pathname == '/Overall_dashboard' or pathname == '/supply_chain':
        navbar_class = 'hidden-navbar'
    else:
        navbar_class = ''  # Show the navbar for other pages

    # Display the corresponding page layout
    page_content = layout.display_page(pathname)
    return page_content, navbar_class

@app.callback(
    Output({'type': 'modal', 'index': MATCH}, 'is_open'),
    Input({'type': 'open-modal', 'index': MATCH}, 'n_clicks'),
    State({'type': 'modal', 'index': MATCH}, 'is_open')
)
def toggle_modal(n_clicks, is_open):
    if n_clicks:
        return not is_open
    return is_open

@app.callback(
    [
        Output(f'navitem-{dashboard}', 'className') for dashboard in dashboards
    ],
    [Input('url', 'pathname')]
)
def update_active_nav_item(pathname):
    # Define the base class for inactive links and an active class for active links
    base_class = 'nav-link'
    active_class = 'nav-link active'
    
    # Handle the case where pathname is '/'
    if pathname == '/':
        return [active_class if dashboard == 'Overall_dashboard' else base_class for dashboard in dashboards]
    
    # Update the class based on the current pathname
    return [
        active_class if pathname == f'/{dashboard}' else base_class for dashboard in dashboards
    ]



@app.callback(
    Output("sidebar", "style"),
    [Input("sidebar-handle", "n_clicks")],
    [State("sidebar", "style")],
)
def toggle_sidebar(n_clicks_handle, sidebar_style):
    print(f"n_clicks_handle: {n_clicks_handle}, sidebar_style: {sidebar_style}")  # Debug output
    
    if n_clicks_handle is None:
        raise PreventUpdate

    if sidebar_style is None:
        sidebar_style = {"transform": "translateX(0)"}

    if sidebar_style.get("transform") == "translateX(0)":
        sidebar_style["transform"] = "translateX(-210px)"  # Move sidebar out of view
    else:
        sidebar_style["transform"] = "translateX(0)"  # Move sidebar into view

    print(f"Updated sidebar_style: {sidebar_style}")  # Debug output
    return sidebar_style

# Register callbacks for each dashboard
for dashboard in [EDI_SAP,Inventory_db,manage_users_layout,Overall_dashboard,reduced_db_manip,Clusters]:
    dashboard.register_callbacks(app)
