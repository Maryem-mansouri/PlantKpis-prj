
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html

# Define the function to create a filter dropdown
def create_filter_dropdown(label, checklist_id, options, default_value=None):
    all_option = {'label': 'Select All', 'value': 'all'}
    option_items = [{'label': option, 'value': option} for option in options]
    return dbc.DropdownMenu(
        label=label,
        children=[
            dbc.DropdownMenuItem(
                dbc.Checklist(
                    id=checklist_id,
                    options=[all_option] + option_items,
                    value=default_value if default_value else options,
                    inline=False,
                    switch=(label == "PPV Reporting"),
                ),
                toggle=False,
                className="bg-white",
                style={'maxHeight': '200px', 'overflowY': 'auto'}
            ),
        ],
        color="light",
        className="me-2"
    )