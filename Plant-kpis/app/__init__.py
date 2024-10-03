import dash
import dash_bootstrap_components as dbc

app = dash.Dash(__name__, external_stylesheets=[
    dbc.themes.BOOTSTRAP,
    "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"
],suppress_callback_exceptions=True,)
app.title = 'Dash App'

from app import layout, callbacks

app.layout = layout.create_layout()
