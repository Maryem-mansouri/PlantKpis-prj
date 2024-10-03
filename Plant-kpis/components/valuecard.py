import dash_html_components as html
import dash_bootstrap_components as dbc
from config.config import format_value

def create_value_card(value_text, value, Week_number):
    formatted_week_number = str(Week_number).strip("[]'")
    return html.Div(
        dbc.Card(
            dbc.CardBody(
                [
                    html.Div(value_text, style={'fontSize': '1em', 'color': '#002060', 'textAlign': 'center', 'width': '100%', 'marginTop': '10px'}),
                    html.Div(f"{format_value(value)}", style={'fontSize': '3em', 'color': '#DE7A00', 'fontWeight': 'bold' ,'textAlign': 'center', 'width': '100%', 'marginTop': '10px'}),
                    html.Div(f'Week {formatted_week_number}', style={'fontSize': '1em', 'color': '#002060', 'textAlign': 'center', 'width': '100%', 'marginTop': '10px'})
                ],
                style={
                    'textAlign': 'center',
                    'background': 'linear-gradient(to bottom right, #FFFFFF, #FFE7CC)',
                    'borderRadius': '50px',
                    'padding': '80px 20px 80px 20px',  # Increased bottom padding
                    'width': '100%',
                    'height': '100%'
                }
            ),
            style={'border': 'none', 'width': '100%'}
        ),
        style={
            'display': 'flex',
            'justifyContent': 'center',
            'alignItems': 'center',
            'height': '100%',
            'width': '100%',
        }
    )

# Example usage:
# app.layout = html.Div(create_value_card("Value of stock", 25400))
