# -*- coding: utf-8 -*-
import dash
import socket
import requests
import os
import logging
import numpy as np
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import plotly.io as pio
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import dash_daq as daq
from front_functions import *
from risk_tab import make_risk_tab
from portfolio_tab import make_portfolio_tab
from issues_tab import (make_issues_tab, 
                        update_vintage_figure, 
                        update_count_figure, 
                        update_sum_figure)
from mmb_tab import (make_mmb_tab, 
                     update_continious_figure,
                     update_hazard_figure,
                     update_rol_figure)
from monitoring_tab import make_monitoring_tab
from tests_tab import make_tests_tab
from tests_page import make_tests_page

logger = logging.getLogger(__name__)

final_config = {"host":"0.0.0.0",
            "port":5002,
            "debug": True,
            "server_host":"10.114.17.86",
            "server_port": 5001
           }

if os.path.exists("./web_service/front_config.cfg"):
    logger.info('config_file exists')
    import json
    with open("./web_service/front_config.cfg", "r") as f:
        config_txt = f.read()
        logger.info('config_file opened')
        config = json.loads(config_txt)
        final_config= {"host":config["host"],
                "port":config["dev_port"] if config['debug'] else config["prom_port"],
                "debug": config["debug"],
                "server_host":config["server_host"],
                "server_port":config["server_port"]}

logger.info('mode debug: ' + ('True' if final_config['debug'] else 'False'))
SERVER_API_ADDRESS = 'http://{0}:{1}/api/'.format(final_config["server_host"],final_config["server_port"])
display_url = (
    socket.gethostname(),
    str(final_config["port"])
)


app = dash.Dash(__name__)
app.config.suppress_callback_exceptions = True

pio.templates.default = 'plotly_white'

colors_dict = {
    'green': '#6ab187',
    'yellow': '#ffffb5',
    'red': '#ff968a',
}
colors_list = ['#4cb5f5', '#dbae58', '#1f3f49', '#7e909a', '#ea6a47', '#0091d5', '#6ab187', '#488a99', '#1c4e80',
               '#a5d8dd', '#ced2cc', '#d32d41', '#b3c100']

# Необходимые сущности

# Отрисовка страницы

app.layout = html.Div([dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
    ])

main_page_layout = html.Div([
    html.Div([
        html.Img(
            src=app.get_asset_url('./logo.png'),
            alt='Risk Modeling Research',
            style={
                'width': '386px',
                'height': '84px',
                'float': 'left',
            }
        ),
        html.Div(
            [
                html.H2(
                    'Анализ портфеля ММБ',
                    style={
                        # 'border':'4px solid black'
                    }
                ),
                html.H4(
                    'Основные показатели портфеля ММБ',
                    style={
                        # 'border':'4px solid red'
                    }
                )
            ],
            className='main_title',
            style={
                'float': 'left',
                'text-align': 'center',
                'width': 'calc(99% - 2*386px + 70px)',
                # 'border':'4px solid red',
            }
        )],
        id='header',
        className='header',
        style={
            'width': '99%',
            'height': 100,
            # 'border':'4px solid blue',
        }
    ),
    html.Div([
        dcc.Tabs(id="tabs_graph", value='risk_tab', children=[
            dcc.Tab(label='Риски', value='risk_tab', className='custom-tab', 
                    selected_className='custom-tab--selected'),
            dcc.Tab(label='Портфель', value='portfolio_tab', className='custom-tab',
                    selected_className='custom-tab--selected'),
            dcc.Tab(label='Выдачи', value='issues_tab', className='custom-tab',
                    selected_className='custom-tab--selected'),
            dcc.Tab(label='Качество данных', value='data_tests', className='custom-tab',
                    selected_className='custom-tab--selected'),
            dcc.Tab(label='ММБ', value='mmb_tab', className='custom-tab',
                    selected_className='custom-tab--selected'),
            dcc.Tab(label='Мониторинг моделей', value='monitoring_tab', className='custom-tab',
                    selected_className='custom-tab--selected'),
        ]),
        html.Div(id='tabs_content', style={'height': '10'})
    ], style={'margin':'5px'}),
])

def make_tests_layout(logger, color, server_api_address):
    return html.Div([
        html.Div([
            html.Img(
                src=app.get_asset_url('./logo.png'),
                alt='Risk Modeling Research',
                style={
                    'width': '386px',
                    'height': '84px',
                    'float': 'left',
                }
            ),
            html.Div(
                [
                    html.H2(
                        'Анализ портфеля ММБ',
                        style={
                        }
                    ),
                    html.H4(
                        'Основные показатели портфеля ММБ',
                        style={
                        }
                    )
                ],
                className='main_title',
                style={
                    'float': 'left',
                    'text-align': 'center',
                    'width': 'calc(99% - 2*386px + 70px)',
                }
            )],
            id='header',
            className='header',
            style={
                'width': '99%',
                'height': 100,
            }
        ),
        html.Div([dcc.Link('Назад к мониторингу', href='/', style={'font-size':'30px'})], style={'text-align':'center'}),
        make_tests_page(logger, color, server_api_address)])

@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/':
        return(main_page_layout)
    elif pathname == '/tests':
        return(make_tests_layout(logger, 'all', SERVER_API_ADDRESS))
    elif pathname == '/tests_red':
        return(make_tests_layout(logger, 'red', SERVER_API_ADDRESS))
    elif pathname == '/tests_yellow':
        return(make_tests_layout(logger, 'yellow', SERVER_API_ADDRESS))
    elif pathname == '/tests_green':
        return(make_tests_layout(logger, 'green', SERVER_API_ADDRESS))
    else:
        return(main_page_layout)

@app.callback(Output('tabs_content', 'children'),
              Input('tabs_graph', 'value'), suppress_callback_exceptions=True)
def render_tabs(tab):
    if (tab == 'risk_tab'):
        return make_risk_tab(logger, SERVER_API_ADDRESS)
    elif (tab == 'portfolio_tab'):
        return make_portfolio_tab(logger, SERVER_API_ADDRESS)
    elif (tab == 'issues_tab'):
        return make_issues_tab(logger, SERVER_API_ADDRESS)
    elif (tab == 'mmb_tab'):
        return make_mmb_tab(logger, SERVER_API_ADDRESS)
    elif (tab == 'monitoring_tab'):
        return make_monitoring_tab(logger, SERVER_API_ADDRESS)
    else:
        return make_tests_tab(logger, SERVER_API_ADDRESS)


@app.callback(
    Output('vintage_by_generation_figure', 'figure'),
    Input('generation_slider', 'value'), suppress_callback_exceptions=True)
def update_figure(selected_years):
    return update_vintage_figure(selected_years, logger, SERVER_API_ADDRESS)

@app.callback(
    Output('count_of_issued_products_scatter_figure', 'figure'),
    Input('count_of_issued_products_dropdown', 'value'), suppress_callback_exceptions=True)
def update_figure(value):
    return update_count_figure(value, logger, SERVER_API_ADDRESS)

@app.callback(
    Output('sum_of_issued_products_scatter_figure', 'figure'),
    Input('sum_of_issued_products_scatter_figure_dropdown', 'value'), suppress_callback_exceptions=True)
def update_figure(value):
    return update_sum_figure(value, logger, SERVER_API_ADDRESS)

@app.callback(
    Output('vintages_scatter_figure', 'figure'),
    [Input('category_checklist', 'value'), Input('entity_radio_items', 'value'),
     Input('route_radio_items', 'value'), Input('axis_radio_item', 'value'),
     Input('count_sum_radio_item', 'value'), Input('ratio_radio_item', 'value'),
     Input('dpd_radio_item', 'value'), Input('generations', 'start_date'),
     Input('generations', 'end_date'), Input('restruct_radio_items', 'value')], suppress_callback_exceptions=True)
def update_figure(category_checklist, entity_radio_items, route_radio_items, axis_radio_item, count_sum_radio_item, 
                  ratio_radio_item, dpd_radio_item, start_date, end_date, restruct_radio_items):
    return update_continious_figure(category_checklist, entity_radio_items, route_radio_items, axis_radio_item, 
                                    count_sum_radio_item, ratio_radio_item, dpd_radio_item, start_date, end_date,
                                    restruct_radio_items, logger, SERVER_API_ADDRESS)

@app.callback(
    Output('hazard_scatter_figure', 'figure'),
    [Input('category_checklist', 'value'), Input('entity_radio_items', 'value'),
     Input('route_radio_items', 'value'), Input('generations', 'start_date'),
     Input('generations', 'end_date'), Input('restruct_radio_items', 'value')], suppress_callback_exceptions=True)
def update_figure(category_checklist, entity_radio_items, route_radio_items, start_date, end_date, restruct_radio_items):
    return update_hazard_figure(category_checklist, entity_radio_items, route_radio_items,
                                start_date, end_date, restruct_radio_items, logger, SERVER_API_ADDRESS)

@app.callback(
    Output('rol_scatter_figure', 'figure'),
    [Input('category_checklist', 'value'), Input('entity_radio_items', 'value'),
     Input('route_radio_items', 'value'), Input('generations', 'start_date'),
     Input('generations', 'end_date'), Input('restruct_radio_items', 'value')], suppress_callback_exceptions=True)
def update_figure(category_checklist, entity_radio_items, route_radio_items, start_date, end_date, restruct_radio_items):
    return update_rol_figure(category_checklist, entity_radio_items, route_radio_items,
                             start_date, end_date, restruct_radio_items, logger, SERVER_API_ADDRESS)

if __name__ == '__main__':
    import os
    display_url = (
        socket.gethostname(),
        str(final_config["port"])
    )
    logger.info("Run on: http://%s:%s/\n", *display_url)
    logger.info("Back looks up at "+ SERVER_API_ADDRESS)
    app.run_server(host=final_config["host"], port=final_config["port"], debug=final_config["debug"])

