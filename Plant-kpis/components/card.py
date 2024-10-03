import dash_html_components as html
import dash_core_components as dcc
import dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
from config.config import format_value
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd 
import plotly.express as px
import plotly.graph_objects as go
from dash.dependencies import ALL
import plotly.express as px
import pandas as pd
def get_figure(chart_type, config, path, barmode_group=False):
    data = config['data']
    cols = config['columns']
    color = cols.get('color', None)
    line = cols.get('line', None)
    y1 = cols.get('y1', None)
    
    if not isinstance(data, pd.DataFrame):
        raise ValueError("Expected 'data' to be a DataFrame")

    if color and not isinstance(color, str):
        raise ValueError("Expected 'color' to be a string")

    custom_colors = ['#F18226', '#3B8AD9', '#FFDB69', '#7BC0F7', '#00ACC1', '#DBC8FF', '#80BE86', '#909CC2', '#707070', '#73B8C8', '#E58FAE', '#E3554C']
   
    chart_func_map = {
        'Pie': px.pie,
        'Bar': px.bar,
        'Line': px.line,
    }
 
    chart_func = chart_func_map.get(chart_type, px.scatter)
 
    # Ensure X-axis days are ordered correctly for "/Inventory3"
    if path == "/Inventory3" and 'Day' in data.columns:
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        try:
            data['Day_Split'] = data['Day'].apply(lambda x: x.split('-')[1] if '-' in x else x)
            data['Day_Split'] = pd.Categorical(data['Day_Split'], categories=days_order, ordered=True)
            data = data.sort_values('Day_Split')
        except Exception as e:
            print(f"Error in splitting and sorting days: {e}")

    fig_params = {'data_frame': data}
 
    if chart_type == 'Pie':
        fig_params['values'] = cols['y']
        fig_params['names'] = color if color else cols['x']
        color_field = color if color else cols['x']
        fig_params['color'] = color_field
        unique_values = data[color_field].unique()
        color_discrete_map = {value: custom_colors[i % len(custom_colors)] for i, value in enumerate(unique_values)}
        fig_params['color_discrete_map'] = color_discrete_map
        fig_params['hole'] = 0.6
    else:
        fig_params['x'] = cols['x']
        fig_params['y'] = cols['y']
        fig_params['text'] = data[cols['y']].apply(format_value)
        if color:
            fig_params['color'] = color
            unique_values = data[color].unique()
        else:
            unique_values = data[cols['x']].unique()
        color_discrete_map = {value: custom_colors[i % len(custom_colors)] for i, value in enumerate(unique_values)}
        fig_params['color_discrete_map'] = color_discrete_map
 
    fig = chart_func(**fig_params)
  
    if chart_type == 'Line':
      # Ensure formatted_text is created for each line individually
      if color:
          for trace, col in zip(fig.data, data[cols['color']].unique()):
              trace_data = data[data[cols['color']] == col]
              formatted_text = trace_data[cols['y']].apply(format_value)
              trace.update(
                  mode='lines+markers+text',
                  text=formatted_text.tolist(),
                  textposition='top center',
              )
      else:
          for trace in fig.data:
              formatted_text = data[cols['y']].apply(format_value)
              trace.update(
                  mode='lines+markers+text',
                  text=formatted_text.tolist(),
                  textposition='top center',
                  line=dict(color=custom_colors[2], width=2),
                  marker=dict(color=custom_colors[0], size=8)
              )
    if chart_type == 'Bar':
        fig.update_traces(
            textposition='auto'
        )
        if color:
            fig.update_traces(
                marker=dict(
                    opacity=1,
                )
            )
        else:
            fig.update_traces(
                marker=dict(
                    color=custom_colors[1],
                    line=dict(color=custom_colors[1], width=1.5),
                    opacity=1,
                    cornerradius = 10
                )
                
            )
        fig.update_layout(barmode='group' if barmode_group else 'stack')
        
 
    fig.update_layout(
        xaxis=dict(
            title=None,
            tickfont=dict(size=10),
            dtick=1  # Ensure that every tick mark is displayed on the x-axis
        ),
        yaxis=dict(title=None, tickfont=dict(size=10)),
        margin=dict(l=10, r=10, t=10, b=10),
        height=320,
        plot_bgcolor='#f2f7fc',  # Background color of the plotting area
        paper_bgcolor='#FFFFFF',
        font_family="sans-serif",
        legend=dict(
            orientation='h',
            y=1,
            xanchor='left',
            x=0,
            bgcolor='rgba(255, 255, 255, 0.25)',
            font=dict(size=8)
        ),
    )
 
    return fig




def create_filter_dropdown(label, checklist_id, options, default_value=None):
    return dbc.DropdownMenu(
        label=label,
        children=[
            dbc.DropdownMenuItem(
                html.Div(
                    dbc.Checklist(
                        id=checklist_id,
                        options=[{'label': option, 'value': option} for option in options],
                        value=default_value if default_value else [],
                        inline=False,
                    ),
                    style={'maxHeight': '200px', 'overflowY': 'auto'}
                ),
                toggle=False,
                className="bg-white"
            ),
        ],
        color="light",
        className="me-2"
    )


def create_modal(index, header, body):
    return dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle(header)),
            dbc.ModalBody(body),
        ],
        id={'type': 'modal', 'index': index},
        size="xl",
        is_open=False,
        style={"maxHeight": "100vh"} 
    )
    
def create_card(title, index, prefix, default_switch_value, default_chart_type, path=None, filter_label=None, filter_id=None, filter_options=None, default_filter_value=None, barmode_group=False, show_switch=True):
    initial_content = html.Div()  # Empty initial content, to be updated later
    switch_element = dbc.Col(
        html.Div([
            html.H6('Graph', className='ml-2 m-2 align-self-center'),
            dbc.Switch(id={'type': f'{prefix}-Graph-switch', 'index': index}, className="mt-1", value=default_switch_value)
        ], className="d-flex justify-content-end align-items-center"),
        style={'display': 'block' if show_switch else 'none'}  # Conditionally hide the switch element
    )
    filter_dropdown = None
    if filter_label and filter_id and filter_options is not None:
        filter_dropdown = create_filter_dropdown(filter_label, filter_id, filter_options, default_filter_value)

    chart_options = [
        {'label': 'Pie', 'value': 'Pie'},
        {'label': 'Bar', 'value': 'Bar'},
        {'label': 'Line', 'value': 'Line'},
    ]

    if index == 3 and path == 'Inventory2':
        chart_options.append({'label': 'Value', 'value': 'Value'})

    return html.Div([
        dbc.Card(
            dbc.CardBody([
                dbc.Row([
                    dbc.Col(html.H6(title, className="Cards-title" , id={'type': f'{prefix}-card-title', 'index': index}), width=6),
                    dbc.Col(filter_dropdown if filter_dropdown else None, width=2),
                    dbc.Col(switch_element, width=4) if switch_element else None,
                ]),
                html.Div(
                    dbc.RadioItems(
                        id={'type': f'{prefix}-chart-type', 'index': index},
                        options=chart_options,
                        value=default_chart_type,
                        inline=True,
                    ),
                    id={'type': f'{prefix}-chart-options', 'index': index},
                    style={'display': 'block' if default_switch_value else 'none'}
                ),
                html.Div(initial_content, id={'type': f'{prefix}-dynamic-content', 'index': index}),
                html.Div(style={"paddingBottom": "30px"}),  # Adding padding bottom to make space for the icon
                html.Div(
                    html.I(className="bi bi-pip", id={'type': 'open-modal', 'index': index}, style={"cursor": "pointer", "fontSize": "24px", "color": "#DDE0E5"}),
                    style={"position": "absolute", "bottom": "10px", "right": "10px"}
                ),
                # Add the checkbox at the bottom left, initially disabled
                dbc.Checkbox(
                    id={'type': 'card-checkbox', 'index': index},
                    className='form-check-input',
                    style={"position": "absolute", "bottom": "10px", "left": "10px"},
                    disabled=True
                ),
            ]),
            style={"height": "25em", "borderRadius": "20px", "position": "relative"},
            className="border-0"
        ),
        create_modal(index, title, html.Div(id={'type': f'{prefix}-dynamic-content-modal', 'index': index})),
        dcc.Store(id={'type': f'{prefix}-barmode-group', 'index': index}, data=barmode_group)  # Store barmode_group value
    ])


def create_data_table(formatted_data, height, style_data_conditional,scrollable=False):
    return dcc.Loading(
        children=[dash_table.DataTable(
            data=formatted_data.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in formatted_data.columns],
            style_table={'height': height, 'overflowY': 'auto'},
            style_cell={
                'textAlign': 'left',
                'padding': '10px',
                'font-size': '14px',
                'border': '1px solid #e0e0e0'
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
            style_data_conditional=style_data_conditional,
            editable=True,
            filter_action="native",
            sort_action="native",
            sort_mode='multi',
            page_action='none' if scrollable else 'native',
            page_current=0,
            page_size=10,
        )], type="default"
    )