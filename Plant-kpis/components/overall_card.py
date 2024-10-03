# components/overall_card.py
import dash_bootstrap_components as dbc
from dash import html
import plotly.graph_objs as go 
from dash import dcc
import plotly.express as px 
import plotly.graph_objs as go
 
def create_gauge_chart(value):
    # Create a gauge chart
    color = "orange" if value < 13000000 else "#6cb284"

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        number={'suffix': "$", 'font': {'size': 30, 'color': 'orange'}},
        gauge={
            'axis': {'range': [0, 16000000], 'tickwidth': 1, 'tickcolor': "gray"},
            'bar': {'color': "rgba(0,0,0,0)"},
            'bgcolor': "white",
            'borderwidth': 0,
            'steps': [
                {'range': [0, 13000000], 'color': "lightgray"},
                {'range': [13000000, 16000000], 'color': "lightgray"},
                {'range': [0, value], 'color': color} 
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': value
            }
        }
    ))
 
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",  # transparent background
        plot_bgcolor="rgba(0,0,0,0)",
        height=100,
        margin=dict(l=0, r=0, t=0, b=0)
    )
 
    return fig
 
def createppv_card(title, subtitle, value):
    fig = create_gauge_chart(value)
   
    card = dbc.Card(
        dbc.CardBody(
            [
                html.H6(title, className="card-title"),
                html.P(subtitle, className="card-text", style={'color': 'grey'}),
                dcc.Graph(figure=fig, config={'displayModeBar': False})
            ],
            style={"background": "linear-gradient(135deg, #ffffff 0%, #FFE7CC 100%)", "borderRadius": "10px"}
        ),
       style={"height": "230px", "borderRadius": "20px"}, className="border-0"
    )
   
    return card

def create_overall_card(title, sub_cards_layouts):
    return dbc.Card(
        [
            dbc.CardHeader([
                html.H4(title, className="card-title", style={"display": "inline-block", "color": "#023047"}),
            dcc.Link(
                    dbc.Button("Details", className="ml-auto", style={"float": "right", "padding": "0.5rem 1rem", "background-color":"#F18226","border":"None"}),
                    href="/Inventory_db",  # Link to the Inventory page
                    style={"text-decoration": "none"}
                )            ], style={"padding": "0.5rem 1rem", "background-color": "white", "border-bottom": "none"}),
            dbc.CardBody(sub_cards_layouts),
        ],
        className="overall-card",
        style={"padding": "10px", "border-radius": "15px", "box-shadow": "0 4px 8px rgba(0,0,0,0.1)", "background-color": "white","border": "none"}
    )

def create_sub_card(title, value, change=None, width=3, height='100px'):
    change_style = {"color": "green"} if change and change > 0 else {"color": "red"}
    return dbc.Col(
        dbc.Card(
            dbc.CardBody([
                html.Div([
                    html.H6(title, className="card-title widget-card-title mb-1", style={"font-size": "0.8em", "display": "inline-block", "color": "#023047"}),
                    html.P(f"{change}%" if change else "", className="sub-card-change", style={**change_style, "display": "inline-block", "float": "right", "margin": "0", "font-size": "0.8em"})
                ], style={"margin-bottom": "5px"}),
                html.P(value, className="sub-card-value", style={"font-size": "1.2em", "font-weight": "bold", "text-align": "center"}),
            ],className="sub-card-body"),
            className="sub-overallcard",
            style={"height": height,   "background": f"url('/assets/bdcard.png') no-repeat center center / cover",}  # Adjust height as needed
        ),
        width=width,
        style={"padding": "0px"}
    )
 
 
 
def create_notification_card():
    return dbc.Card(
        [
            dbc.CardHeader(
                html.H4("Notification", className="notification-title", style={"color": "#023047"})
            ),
            dbc.CardBody(
                [
                    html.H6("27/08/2024"),
                    dbc.Alert(
                        [
                            html.I(className="bi bi-info-circle-fill me-2"),
                            html.H6("The target for the 'CAS' team has been updated to 15%", className="alert-text"),
                        ],
                        color="primary",
                        className="d-flex align-items-center",
                    ),
                    html.H6("07/08/2024"),
                    dbc.Alert(
                        [
                            html.I(className="bi bi-check-circle-fill me-2"),
                            html.H6("The 'Inventory' has reached the monthly target !", className="alert-text"),
                        ],
                        color="success",
                        className="d-flex align-items-center",
                    ),
                    html.H6("20/07/2024"),
                    dbc.Alert(
                        [
                            html.I(className="bi bi-exclamation-triangle-fill me-2"),
                            html.H6("The 'VW' Project is below the target of 20%", className="alert-text"),
                        ],
                        color="warning",
                        className="d-flex align-items-center",
                    ),
                     html.H6("17/07/2024"),
                    dbc.Alert(
                        [
                            html.I(className="bi bi-exclamation-triangle-fill me-2"),
                            html.H6("The 'VW' Project is below the target of 20%", className="alert-text"),
                        ],
                        color="warning",
                        className="d-flex align-items-center",
                    ),
                    html.H6("12/07/2024"),
                    dbc.Alert(
                        [
                            html.I(className="bi bi-check-circle-fill me-2"),
                            html.H6("The 'Inventory' has reached the monthly target !", className="alert-text"),
                        ],
                        color="success",
                        className="d-flex align-items-center",
                    ),
                ]
            ),
        ],
        className="notification-card",
        style={
            "padding": "20px",
            "border-radius": "10px",
            "box-shadow": "0 4px 8px rgba(0,0,0,0.1)",
            "border": "none"
        }
    )


def create_graph_card(data, x, y, color=None, title="Graph", width=12, height='230px'):
    # Create the figure with Plotly Express
    fig = px.bar(data, x=x, y=y, color=color, title=title)
    fig.update_traces(marker_color='rgba(47, 169, 161, 1)')
    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),  # Adjust margins
        height=200,  # Adjust the height of the graph itself
        title_x=0.5,  # Center the title
        showlegend=False,  # Hide the legend if it's not necessary
        plot_bgcolor='rgba(0, 0, 0, 0)',  # Transparent background
        paper_bgcolor='rgba(0, 0, 0, 0)',  # Transparent paper background
        font=dict(size=10),  # Adjust the font size for better readability
    )
    
    return dbc.Col(
        dbc.Card(
            dbc.CardBody([
                dcc.Graph(figure=fig, config={"displayModeBar": False}),
            ]),
            className="graph-card",
            style={
                "padding": "5px", 
                "border-radius": "5px", 
                "box-shadow": "0 2px 4px rgba(0,0,0,0.1)", 
                "margin": "5px", 
                "height": height, 
                "border": "1px solid #d9d9d9",  # Adds a border similar to the sub-cards
                "background-color": "white"  # Ensures the background is white like the sub-cards
            }
        ),
        width=width,
        style={"padding": "0px"}
    )


def create_graph_card_line(data, x, y, color=None, title="Graph", width=12, height='230px'):
    # Create the figure with Plotly Express
    fig = px.line(data, x=x, y=y, color=color, title=title)
    fig.update_traces(marker_color='rgba(47, 169, 161, 1)')
    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),  # Adjust margins
        height=200,  # Adjust the height of the graph itself
        title_x=0.5,  # Center the title
        showlegend=False,  # Hide the legend if it's not necessary
        plot_bgcolor='rgba(0, 0, 0, 0)',  # Transparent background
        paper_bgcolor='rgba(0, 0, 0, 0)',  # Transparent paper background
        font=dict(size=10),  # Adjust the font size for better readability
    )
    
    return dbc.Col(
        dbc.Card(
            dbc.CardBody([
                dcc.Graph(figure=fig, config={"displayModeBar": False}),
            ]),
            className="graph-card",
            style={
                "padding": "5px", 
                "border-radius": "5px", 
                "box-shadow": "0 2px 4px rgba(0,0,0,0.1)", 
                "margin": "5px", 
                "height": height, 
                "border": "1px solid #d9d9d9",  # Adds a border similar to the sub-cards
                "background-color": "white"  # Ensures the background is white like the sub-cards
            }
        ),
        width=width,
        style={"padding": "0px"}
    )


def create_pie_chart_card(data, names, values, title="Pie Chart", width=12, height='230px'):
    # Create the pie chart with Plotly Express
    fig = px.pie(data, names=names, values=values, title=title)
    
    # Customize the pie chart style
    fig.update_traces(marker=dict(colors=['rgba(47, 169, 161, 1)', 'rgba(255, 123, 123, 1)', 'rgba(255, 195, 0, 1)', 'rgba(85, 85, 255, 1)']),
                      textinfo='percent+label')  # Show percentage and label in the pie
    
    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),  # Adjust margins
        height=200,  # Adjust the height of the graph itself
        title_x=0.5,  # Center the title
        showlegend=False,  # Hide the legend if it's not necessary
        plot_bgcolor='rgba(0, 0, 0, 0)',  # Transparent background
        paper_bgcolor='rgba(0, 0, 0, 0)',  # Transparent paper background
        font=dict(size=10),  # Adjust the font size for better readability
    )
    
    return dbc.Col(
        dbc.Card(
            dbc.CardBody([
                dcc.Graph(figure=fig, config={"displayModeBar": False}),
            ]),
            className="graph-card",
            style={
                "padding": "5px", 
                "border-radius": "5px", 
                "box-shadow": "0 2px 4px rgba(0,0,0,0.1)", 
                "margin": "5px", 
                "height": height, 
                "border": "1px solid #d9d9d9",  # Adds a border similar to the sub-cards
                "background-color": "white"  # Ensures the background is white like the sub-cards
            }
        ),
        width=width,
        style={"padding": "0px"}
    )