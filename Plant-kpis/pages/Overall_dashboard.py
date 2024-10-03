# Overall_dashboard.py
import sys
import os
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dash import dcc, html
import dash_bootstrap_components as dbc
from components.sidebar import sidebar
import datetime  # Import datetime module
from components.overall_card import create_pie_chart_card,create_graph_card_line,createppv_card, create_overall_card, create_sub_card, create_notification_card,create_graph_card

# Your data for the graph
data = pd.DataFrame({
    "Week": ["WK-21", "WK-22", "WK-23", "WK-24", "WK-25","WK-26","WK-27"],
    "Value": [14.8, 13.2, 12.5, 13.0, 15.9,12.6,13.9],
    "Color": ["orange"] * 7  # Use color if necessary
})
graph_title = "Inventory Trend"

dataedi = pd.DataFrame({
    "Week": ["WK-21", "WK-22", "WK-23", "WK-24", "WK-25","WK-26","WK-27"],
    "Value": [20.8, 13.2, 17.5, 19.0, 15.9,16.6,14.9],
    "Color": ["orange"] * 7  # Use color if necessary
})
graph_titleedi = "Weekly Edi Variation"

data2 = pd.DataFrame({
    "Project": ["FORD", "HSD", "SQUIB", "Tableland", "VW","Tesla", "VW2"],
    "Avg_Demand": [53.92, 435.40, 1040.0, 70.10, 853.21,75.10, 653.21],  # In K$ or M$
    "Color": ["orange"] * 7  # Optional color field
})
graph_title2 = "AVG demand $ by project"

data3 = pd.DataFrame({
   "Consumption Type": [
        "Con 0-2 weeks $",
        "Con 3-4 weeks $",
        "Con 5-6 weeks $", 
        "Con 7-8 weeks $", 
        "Con >8 weeks $", 
        "Obsolete $"
    ],


    "Value": [
        697.95, 1230.0, 756.45, 474.46, 624.21, 506.04, 
    ]
})
graph_title3 = "AVG demand $ by project"
# Function to get the current date with the correct ordinal suffix
def get_current_date_with_suffix():
    today = datetime.datetime.now()
    day = today.day

    # Determine the suffix for the day
    if 4 <= day <= 20 or 24 <= day <= 30:
        suffix = "th"
    else:
        suffix = ["st", "nd", "rd"][day % 10 - 1]

    formatted_date = today.strftime(f"%b, {day}{suffix}, %Y")
    return formatted_date

# Get the formatted date
current_date = get_current_date_with_suffix()

def layout():
    return html.Div(
        className="overall-dashboard",
        id="main-container",
        children=[
            html.Div(
                className="header-background",
                children=[
                    dbc.Row([
                        dbc.Col(html.Img(src="/assets/image-removebg-preview 1.png", style={"height": "60px", "display": "block", "margin-left": "auto", "margin-right": "auto"}), width=2),
                        dbc.Col(html.H3(current_date, style={"text-align": "center", "font-size": "1.5em", "color": "#023047"}), width=8, className="d-flex align-items-center justify-content-center"),
                        dbc.Col(
                            html.Div(
                                className="user-info d-flex justify-content-end align-items-center",
                                children=[
                                    html.Div(
                                        [
                                            html.P("Noha HASSOUNI", className="user-name", style={"margin-bottom": "0", "font-weight": "bold", "font-size": "0.8em"}),
                                            html.P("Logistics SPV", className="user-role", style={"margin-bottom": "0", "font-size": "0.7em"}),
                                            html.P("Supply chain Department", className="user-department", style={"margin-top": "0", "font-size": "0.7em"})
                                        ],
                                        style={"padding-right": "5px","padding-top": "10px"}
                                    ),
                                    html.Img(
                                        src="/assets/user_image.png",
                                        className="user-image",
                                        style={
                                            "height": "50px",
                                            "border-radius": "50%",
                                            "vertical-align": "middle",
                                            "margin-right": "10px"
                                
                                        }
                                    ),
                                ]
                            ), width=2),
                    ], align="center", style={"margin-bottom": "40px"})
                ],
            ),
            html.Div(
                className="content-background",
                children=[
                    dbc.Row([
                        dbc.Col(create_overall_card("Overall Dashboard",
                            dbc.Container([
                                dbc.Row([
                                    create_sub_card("Today's Inventory Value", "13.9M$", None),
                                    create_sub_card("Today's Inv Value(1249)", "3.33M$", 11.2),
                                    create_sub_card("Top Project", "SQUIB (1.09M$)", 10),
                                    create_sub_card("Top Project on Demand", "VW", -5)
                                ]),
                                dbc.Row([
                                    dbc.Col([
                                        dbc.Row([
                                             create_graph_card(data, x="Week", y="Value", color="Color", title=graph_title, height='230px')
                                        ])
                                    ], width=6),
                                    dbc.Col([
                                            createppv_card(
                                                title="Inventory Value vs target",
                                                subtitle="Above target by 1.84M$",
                                                value=14000000  # Assuming the value is in thousands for the gauge chart
                                            )
                                        ], width=6),

                                ]),
                                dbc.Row([
                                    dbc.Col([
                                             create_pie_chart_card(data3, names="Consumption Type", values="Value", title=graph_title3,width=12, height='230px')

                                        ], width=4),
                                    dbc.Col([
                                        dbc.Row([
                                             create_graph_card(dataedi, x="Week", y="Value", color="Color", title=graph_titleedi, width=12,height='230px')
                                        ])
                                    ], width=4),
                                    dbc.Col([
                                             create_graph_card_line(data2, x="Project", y="Avg_Demand", color="Color", title=graph_title2,width=12, height='230px')

                                        ], width=4),
                                   
                                ]),
                            ])
                        ), width=9),
                        dbc.Col(create_notification_card(), width=3),
                    ])
                ],
                style={"padding": "10px"}
            )
        ],
        style={
            "height": "50vh",
            "padding": "0",
            "background": "linear-gradient(135deg, rgba(255, 165, 0, 0.5), rgba(255, 165, 0, 0) 40%), "
                         "linear-gradient(225deg, rgba(47, 169, 161, 0.5), rgba(0, 255, 255, 0) 40%)",
        }
    )



def register_callbacks(app):
    pass  