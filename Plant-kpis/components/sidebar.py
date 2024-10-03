from dash import html
import dash_bootstrap_components as dbc

sidebar = html.Div(
    [
        html.Img(
            src="/assets/image-removebg-preview 1.png",
            style={"height": "60px", "width": "auto", "padding": "10px"}
        ),
        
        html.Div(
            [
                dbc.NavLink("Overall dahboard", href="/Overall_dashboard", id="navitem-Overall_dashboard", className="nav-link"),
                dbc.NavLink("Mapping Files",  href="/reduced_db_manip", id="navitem-reduced_db_manip", className="nav-link"),
                dbc.NavLink("User Management", href="/ManageUsers", id="navitem-manage_users_layout", className="nav-link"),
                html.Div(
                    
                    [   
                        dbc.DropdownMenu(
                            [
                                dbc.DropdownMenuItem("Inventory", href="/Inventory_db", id="navitem-Inventory_db"),
                                dbc.DropdownMenuItem("Client Demand",href="/EDI_SAP", id="navitem-EDI_SAP"),
                                dbc.DropdownMenuItem("Clusters",href="/Clusters", id="navitem-Clusters"),

                                
                            ],
                            label="All Dashboards",
                            nav=True,
                            in_navbar=False,
                            className="w-100",
                        ),
                    ],
                    id="dropdown-container",
                    style={"width": "100%"}
                ),
            ],
            id="sidebar-content",
            style={"display": "block"},
        ),
        html.Div(
            "Sidebar", id="sidebar-handle", className="sidebar-handle"
        ),
    ],
    id="sidebar",
    className="sidebar"
)
