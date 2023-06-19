# -*- coding: utf-8 -*-
import dash
import socket
import requests
import logging
import numpy as np
from dash import dcc
from dash import html, dash_table
from dash.dependencies import Input, Output
from datetime import date
import plotly.graph_objs as go
import plotly.io as pio
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import dash_daq as daq
import numpy as np

pio.templates.default = 'plotly_white'

# API_ADDRESS = 'http://10.114.17.86:5001/api2/'
colors_dict = {
    'green': '#6ab187',
    'yellow': '#ffffb5',
    'red': '#ff968a',
}

# colors_list = ['#005083', '#0064ab', '#6876a4', '#c99f6e', '#ab8339', '#f0b64d', '#9f8a89', '#ffd28f', '#ffd7b0']
colors_list = ['#4cb5f5', '#dbae58', '#1f3f49', '#7e909a', '#ea6a47', '#0091d5', '#6ab187', '#488a99', '#1c4e80',
               '#a5d8dd', '#ced2cc', '#d32d41', '#b3c100']

def request_risk_portfolio_metrics(logger, api_address):
    try:
        risk_portfolio_metrics_response = requests.get(api_address + 'risk/portfolio/metrics')
        logger.info('Загрузили risk_portfolio_metrics')
        return pd.DataFrame(risk_portfolio_metrics_response.json()['data']).sort_values('report_dt')
    except:
        logger.warning('Не загрузили risk_portfolio_metrics')
        return None

def request_risk_portfolio_products(logger, api_address):
    try:
        risk_portfolio_products_response = requests.get(api_address + 'risk/portfolio/products')
        logger.info('Загрузили risk_portfolio_products')
        return pd.DataFrame(risk_portfolio_products_response.json()['data']).sort_values('report_dt')
    except:
        logger.warning('Не загрузили risk_portfolio_products')
        return None
            
def request_portfolio_balances(logger, api_address):
    try:
        portfolio_balances_response = requests.get(api_address + 'portfolio/balances')
        logger.info('Загрузили portfolio_balances')
        return pd.DataFrame(portfolio_balances_response.json()['data']).sort_values('report_dt')
    except:
        logger.warning('Не загрузили portfolio_balances')
        return None
        
def request_portfolio_products(logger, api_address):
    try:
        portfolio_products_response = requests.get(api_address + 'portfolio/products')
        logger.info('Загрузили portfolio_products')
        return pd.DataFrame(portfolio_products_response.json()['data']).sort_values('report_dt')
    except:
        logger.warning('Не загрузили portfolio_products')
        return None
        
def request_issues_products(logger, api_address):
    try:
        issues_products_response = requests.get(api_address + 'issues/products')
        logger.info('Загрузили issues_products')
        issues_products = pd.DataFrame(issues_products_response.json()['data']).sort_values('generation')
        issues_products['report_dt'] = issues_products['generation']
        return issues_products
    except:
        logger.warning('Не загрузили issues_products')
        self.issues_products = None
            
def request_portfolio_npl(logger, api_address):
    try:
        portfolio_npl_response = requests.get(api_address + 'portfolio/npl')
        logger.info('Загрузили portfolio_npl')
        return pd.DataFrame(portfolio_npl_response.json()['data']).sort_values('report_dt')
    except:
        logger.warning('Не загрузили portfolio_npl')
        return None
            
def request_portfolio_dr(logger, api_address):
    try:
        portfolio_dr_response = requests.get(api_address + 'portfolio/dr')
        logger.info('Загрузили portfolio_dr')
        return pd.DataFrame(portfolio_dr_response.json()['data']).sort_values('report_dt')
    except:
        logger.warning('Не загрузили portfolio_dr')
        return None
        
def request_issues_vintage(logger, api_address):
    try:
        issues_vintage_response = requests.get(api_address + 'issues/vintage')
        logger.info('Загрузили issues_vintage')
        return pd.DataFrame(issues_vintage_response.json()['data']).sort_values(['generation', 'mob'])
    except:
        logger.warning('Не загрузили issues_vintage')
        return None
            
def request_dqa_portfolio(logger, api_address):
    try:
        dqa_portfolio_response = requests.get(api_address + 'dqa/portfolio')
        logger.info('Загрузили dqa_portfolio')
        return pd.DataFrame(dqa_portfolio_response.json()['data'])
    except:
        logger.warning('Не загрузили dqa_portfolio')
        return None
            
def request_dqa_issues(logger, api_address):
    try:
        dqa_issues_response = requests.get(api_address + 'dqa/issues')
        logger.info('Загрузили dqa_issues')
        return pd.DataFrame(dqa_issues_response.json()['data'])
    except:
        logger.warning('Не загрузили dqa_issues')
        return None

def request_dqa_comparisson(logger, api_address):
    try:
        dqa_comparisson_response = requests.get(api_address + 'dqa/comparisson')
        logger.info('Загрузили dqa_comparisson')
        return pd.DataFrame(dqa_comparisson_response.json()['data'])
    except:
        logger.warning('Не загрузили dqa_comparisson. API_ADDRESS = {}'.format(api_address))
        return None
    
def request_mmb_portfolio_metrics(logger, api_address):
    try:
        mmb_portfolio_vintages = requests.get(api_address + 'mmb/portfolio_vintages')
        logger.info('Загрузили mmb/portfolio_vintages')
        return pd.DataFrame(mmb_portfolio_vintages.json()['data'])
    except:
        logger.warning('Не загрузили mmb/portfolio_vintages. API_ADDRESS = {}'.format(api_address))
        return None

def request_mmb_portfolio_hazard(logger, api_address):
    try:
        mmb_portfolio_hazard = requests.get(api_address + 'mmb/portfolio_hazard')
        logger.info('Загрузили mmb/portfolio_hazard')
        return pd.DataFrame(mmb_portfolio_hazard.json()['data'])
    except:
        logger.warning('Не загрузили mmb/portfolio_hazard. API_ADDRESS = {}'.format(api_address))
        return None

class Metric_container():

    def __init__(self, name, data, label, good_ascending=True, value_type='None'):
        '''
        value_type values: percent, mln, bil
        good_ascending affects color of delta
        '''
        self.label = name
        self.data = data.sort_values('report_dt').loc[:, label]
        self.report_dt = data.sort_values('report_dt').loc[:, 'report_dt']
        self.good_ascending = good_ascending

        self.current_value = self.data.iloc[-1]
        self.delta_last_value = self.current_value - self.data.iloc[-2]
        if (good_ascending != (self.delta_last_value >= 0)):
            self.color = colors_dict['red']
        else:
            self.color = colors_dict['green']

        if (value_type == 'percent'):
            self.current_value_human = round(self.current_value * 100, 2)
            self.delta_last_value_human = round(self.delta_last_value * 100, 2)
            self.human_explain = '%'
        elif (value_type == 'mln'):
            self.current_value_human = round(self.current_value / 1000000, 2)
            self.delta_last_value_human = round(self.delta_last_value / 1000000, 2)
            self.human_explain = 'млн'
        elif (value_type == 'bil'):
            self.current_value_human = round(self.current_value / 1000000000, 2)
            self.delta_last_value_human = round(self.delta_last_value / 1000000000, 2)
            self.human_explain = 'млрд'
        else:
            self.current_value_human = round(self.current_value, 2)
            self.delta_last_value_human = round(self.delta_last_value, 2)
            self.human_explain = ''

    def make_metric_field(self, container_id, className, tooltip_text=''):
        self.metric_field_dash = html.Div([
            html.H6('Метрика', style={'color': self.color}),
            html.H6(self.label),
            html.H6(str(self.current_value_human) + ' ' \
                    + '(' + ('+' if self.delta_last_value > 0 else '') \
                    + str(self.delta_last_value_human) + ')' + ' ' + self.human_explain),
            html.Span(tooltip_text, className='tooltiptext'),
        ],
            id=container_id,
            className=className,
            style={'background-color': self.color},
        )

    def make_gauge_field(self, figure_id, className, tooltip_text='', min_value=0, max_value=100,
                         color={'gradient': True, 'ranges': {'#000000': [0, 100]}}):
        self.gauge = daq.Gauge(
            id=figure_id,
            size=170,
            min=min_value,
            max=max_value,
            # label={'label':self.label, 'style':{'font-size':24}},
            # showCurrentValue=True,
            units=self.human_explain,
            value=self.current_value_human,
            # style={'font-size':50, 'color':'#000000'},
            color=color,
        )
        self.gauge_block = html.Div([
            self.gauge,
            html.H6(str(self.current_value_human) + self.human_explain),
            html.H6('(' + ('+' if self.delta_last_value > 0 else '') + str(self.delta_last_value_human) + ')',
                    style={'color': self.color}),
            html.H6(self.label),
            html.Span(tooltip_text, className='tooltiptext')
        ],
            id=figure_id + '_container',
            className=className,
            style={'height': '300px'},
        )

    def make_scatter_field(self, figure_id, className, name, color='#fac1b7'):
        self.scatter = go.Scatter(
            x=self.report_dt,
            y=self.data,
            mode='lines',
            name=name,
            line=dict(
                shape="spline",
                smoothing=1,
                width=2,
                color=color
            ), )
        self.scatter_figure = go.Figure(data=[self.scatter])
        self.scatter_figure.update_layout(title=name)


class Pie_figure():

    def __init__(self, df, name, figure_id):
        self.labels = df.columns
        self.labels = df.columns
        self.pie_fig_count = go.Figure(data=[go.Pie(
            labels=list(portfolio_state_data['products_count'].keys()),
            text=list(portfolio_state_data['products_count'].keys()),
            values=list(portfolio_state_data['products_count'].values()),
            # hoverinfo="value",
            textinfo="text+percent",
            hole=0.8,
            marker=dict(
                colors=['#ffccb6', '#ffffb5', '#f3b0c3', '#fed7c3', '#f6eac2', '#ffaea5', '#ffd8b3', '#ffc8a2',
                        '#fcb9aa', '#ffdbcc'],
            ), )])


class Divided_by_param_figures():

    def __init__(self, data, split_column, value_column, argument_column='report_dt'):
        self.data = data.sort_values(argument_column)[[split_column, value_column, argument_column]]
        self.split_column = split_column
        self.value_column = value_column
        self.argument_column = argument_column
        self.values_list = self.data[self.split_column].drop_duplicates().values
        self.figures = []

    def make_scatter_figure(self, title, stackgroup=None, line_width=0.5, colors_list=colors_list, groupnorm=None):
        self.scatter_fig = go.Figure()
        values_list = self.values_list
        for i, value in enumerate(values_list):
            cur_data = self.data.loc[self.data[self.split_column] == value].sort_values(by=self.argument_column)
            self.scatter_fig.add_trace(go.Scatter(
                x=cur_data[self.argument_column],
                y=cur_data[self.value_column].values,
                # hoverinfo='x+y',
                mode='lines',
                opacity=0.8,
                line=dict(width=line_width,color=colors_list[i]),
                stackgroup=stackgroup,
                groupnorm=groupnorm,
                name=str(value)
            ))
        self.scatter_fig.update_layout(title=title)
        self.figures.append(self.scatter_fig)

    def make_pie_figure(self, title):
        pie_data = self.data[self.data[self.argument_column] == self.data[self.argument_column].max()]
        self.pie_fig = go.Figure(data=[go.Pie(
            labels=pie_data[self.split_column],
            text=pie_data[self.split_column],
            values=pie_data[self.value_column],
            textinfo="text+percent",
            hole=0.8,
            opacity=0.8,
            marker=dict(
                colors=colors_list,
            ), )])
        self.pie_fig.update_layout(title=title)
        self.figures.append(self.pie_fig)

    def make_barchart_figure(self, title):
        self.barchart_fig = px.bar(self.data[self.data[self.argument_column] == self.data[self.argument_column].max()],
                                   x=self.split_column, y=self.value_column)
        self.barchart_fig.update_layout(title=title)
        self.barchart_fig.update_traces(marker_color='#4cb5f5')
        self.figures.append(self.barchart_fig)
        
    def make_scatter_continious_figure(self, title, stackgroup=None, line_width=0.5, colors_list=colors_list, groupnorm=None):
        values_list = self.data[self.argument_column].drop_duplicates().values
        self.scatter_fig = go.Figure()
        for i, value in enumerate(values_list):
            cur_data = self.data.loc[self.data[self.argument_column] == value].sort_values(by=self.split_column)
            self.scatter_fig.add_trace(go.Scatter(
                x=cur_data[self.split_column],
                y=cur_data[self.value_column].values,
                mode='lines',
                opacity=0.8,
                line=dict(width=line_width,color=colors_list[i]),
                stackgroup=stackgroup,
                groupnorm=groupnorm,
                name=str(value)
            ))
            self.scatter_fig.update_layout(title=title)
            self.scatter_fig.update_traces(hovertemplate="<br>".join([
                "mob: %{x}",
                "знач: %{y}"
            ]))
        self.figures.append(self.scatter_fig)
        
    def make_scatter_hazard_figure(self, title, stackgroup=None, line_width=0.5, colors_list=colors_list, groupnorm=None):
        values_list = self.data[self.argument_column].drop_duplicates().values
        self.scatter_fig = go.Figure()
        for i, value in enumerate(values_list):
            cur_data = self.data.loc[self.data[self.argument_column] == value].sort_values(by=self.split_column)
            array = [i for i in range(len(cur_data[self.split_column]))]
            self.scatter_fig.add_trace(go.Scatter(
                x=cur_data[self.split_column],#array
                y=cur_data[self.value_column].values,
                mode='lines',
                opacity=0.8,
                line=dict(width=line_width,color=colors_list[i]),
                stackgroup=stackgroup,
                groupnorm=groupnorm,
                name=str(value)
            ))
            self.scatter_fig.update_layout(title=title)
        self.figures.append(self.scatter_fig)

    def make_dash(self):
        self.dashboard = html.Div([
            html.Div([
                dcc.Graph(
                    id=self.split_column + '_' + self.value_column + '_scatter_figure',
                    figure=self.figures[0],
                )
            ],
                id=self.split_column + '_' + self.value_column + '_scatter_figure_container',
                className="pretty_container three columns",
            ),
            html.Div([
                dcc.Graph(
                    id=self.split_column + '_' + self.value_column + '_pie_figure',
                    figure=self.figures[1],
                )
            ],
                id=self.split_column + '_' + self.value_column + '_pie_figure_container',
                className="pretty_container two columns",
            ),
        ],
            className='full screen',
            style={
                # 'border':'    4px solid green',
                'height': '400px'
            })    
        

def make_colormap(size):
    from random import seed 
    from random import randint
    seed(42)
    lst=[]
    for i in range(size):
        r = randint(0,255)
        g = randint(0,255)
        b = randint(0,255)
        lst.append('#%02x%02x%02x' % (r,g,b))
    return(lst)

def make_tests_field(tests_df, color='all'):
    test_field_list = []
    if color != 'all':
        tests_df_specified = tests_df[tests_df['color'] == color].reset_index(drop=True)
    else:
        tests_df_specified = tests_df.reset_index(drop=True)
    for i in range(tests_df_specified.shape[0]):
        color, name, description = tests_df_specified.loc[i, ['color', 'name', 'description']].values
        color_dict = {
        "green":"green_circle", 
        "red":"red_circle",
        "yellow":"yellow_circle"
        }
        test_field_list.append(html.Div([html.Div(id=color_dict[color]),
            html.H4(name, style={'text-align':'center'}),
            html.H5(description, style={'text-align':'center'}),
            ],
            className="pretty_container full screen",
            style={"height":"100px", "margin-top":"10px"},
            id=str(i)+"_test_field"
            ))
    return test_field_list

def make_tests_agg_field(tests_df):
    traffic_light_series = tests_df.loc[:,'color'].value_counts()
    color_dict = {
        "green":"green_circle", 
        "red":"red_circle",
        "yellow":"yellow_circle"
        }
    return html.Div([
                html.Br(),
                html.Div(html.H2('Результаты тестов'), style={'text-align':'center'}),
                html.Br(),
                html.Div([html.H4('Аггрегированные результаты тестов по цвету светофора. Более подробную информацию можно узнать по '),\
                          dcc.Link('ссылке', href='/tests', style={'font-size':'25px'})], style={'text-align':'center'}),
                html.Br(),

                html.Div([html.Div([html.Div(id='red_circle'),\
                                    dcc.Link(html.H4(traffic_light_series.loc['red'] if 'red' in traffic_light_series.index else 0, \
                                                     style={'padding-left': '100px', 'padding-top':'35px'}), href='/tests_red'),], \
                                   className='metrics_container pretty_container', style={'height':'100px'}),
                          html.Div([html.Div(id='yellow_circle'),\
                                    dcc.Link(html.H4(traffic_light_series.loc['yellow'] if 'yellow' in traffic_light_series.index else 0, \
                                                     style={'padding-left': '100px', 'padding-top':'35px'}), href='/tests_yellow'),], \
                                   className='metrics_container pretty_container', style={'height':'100px'}),
                          html.Div([html.Div(id='green_circle'), \
                                    dcc.Link(html.H4(traffic_light_series.loc['green'] if 'red' in traffic_light_series.index else 0, \
                                                     style={'padding-left': '100px', 'padding-top':'35px'}), href='/tests_green'),], \
                                   className='metrics_container pretty_container', style={'height':'100px'}),
            ]),
           ], className='pretty_container two columns', style={'height':'350px', 'background-color':'#fbfbfb'})

def make_portfolio_slice_field():
    return html.Div([
               html.Br(),
               html.Label("Выбор даты:"),
               dcc.DatePickerRange(
                   id = "generations",
                   min_date_allowed=date(2010, 1, 1),
                   max_date_allowed=date(2022, 11, 30),
                   start_date=date(2010, 1, 1),
                   end_date=date(2022, 11, 30),
                   initial_visible_month = date(2010, 1, 1),
               ),
               html.Br(),
               html.Label("Категории продуктов:"),
               # значения для category_checklist взяты из файла data_collector из функции encode_categories
               dcc.Checklist(
                   id = "category_checklist",
                   options = [
                       {'label': 'Старые технологии', "value": 1},
                       {'label': 'Карта', "value": 2},
                       {'label': 'Оборотный', "value": 3},
                       {'label': 'Инвестиционный', "value": 4},
                       {'label': 'DEGL', "value": 5},
                       {'label': 'Овердрафт', "value": 6},
                       {'label': 'Риск на актив', "value": 7},
                       {'label': 'Гарантия', "value": 8}
                   ],
                   value = [1, 2, 3, 4, 5, 6, 7, 8]),
               html.Label("Категория бизнеса:"),
               dcc.RadioItems(
                   id = "entity_radio_items",
                   options = [
                       {'label': 'ИП', "value": 'IP'},
                       {'label': 'ЮЛ', "value": 'UL'},
                       {'label': 'Оба вида', "value": 'Both'}
                   ],
                   value = 'Both'),
               html.Label("Зона кредита:"),
               dcc.RadioItems(
                   id = "route_radio_items",
                   options = [
                       {'label': 'Белая зона', "value": 'white'},
                       {'label': 'Не белая зона', "value": 'gray'},
                       {'label': 'Оба вида зон', "value": 'Both'}
                   ],
                   value = 'Both'),
               html.Label("Реструктаризации кредита после 2021-01-01:"),
               dcc.RadioItems(
                   id = "restruct_radio_items",
                   options = [
                       {'label': 'С реструктаризациями', "value": "restructed"},
                       {'label': 'Без реструктаризаций', "value": 'not restructed'},
                       {'label': 'Оба вида', "value": 'Both'}
                   ],
                   value = 'Both'),
               ],
               className='full screen', style={'display': 'block', 'height':'300px', 'background-color':'#fbfbfb'}
           )

def make_vintages_slice_field():
    return html.Div([
               html.Br(),
               html.Label("Изменение подсчетов"),
               dcc.RadioItems(
                   id = "count_sum_radio_item",
                   options = [
                       {'label': 'Винтажи по количеству', "value": True},
                       {'label': 'Винтажи по суммам', "value": False}
                   ],
                   value = True, labelStyle={'display': 'block'}),
               html.Label("Изменение соотношения"),
               dcc.RadioItems(
                   id = "ratio_radio_item",
                   options = [
                       {'label': 'Считать в абсолютных значениях', "value": False},
                       {'label': 'Считать долю по винтажам', "value": True}
                   ],
                   value = False, labelStyle={'display': 'block'}),
               html.Label("Количество дней в просрочке:"),
               dcc.RadioItems(
                   id = "dpd_radio_item",
                   options = [
                       {'label': '1+', "value": "1"},
                       {'label': '30+', "value": "30"},
                       {'label': '60+', "value": "60"},
                       {'label': '90+', "value": "90"}
                   ],
                   value = "1", labelStyle={'display': 'block'}),
               html.Label("Смена осей"),
               dcc.RadioItems(
                   id = "axis_radio_item",
                   options = [
                       {'label': 'X_Мобы', "value": True},
                       {'label': 'X_Поколения', "value": False}
                   ],
                   value = True, labelStyle={'display': 'block'}),
               ],
               className='pretty_mmb_portfolio_slice_container', style={'height':'450px', 'background-color':'#fbfbfb'}
           )

def make_monitoring_table_field(df):
    return html.Div([
               html.Br(),
               html.Label("Выбор даты:"),
               dash_table.DataTable(data=df.to_dict("records"), \
                                    columns=[{"name": i, "id":i} for i in df.columns],\
                                    style_data_conditional=[
                                    {
                                        'if': {
                                            'filter_query': '{traffic_light} = 3',
                                            'column_id': "traffic_light"
                                        },
                                        'backgroundColor': 'tomato',
                                        'color': "white"
                                    },
                                    {
                                        'if': {
                                            'filter_query': '{traffic_light} = 2',
                                            'column_id': "traffic_light"
                                        },
                                        'backgroundColor': 'yellow',
                                        'color': "blach"
                                    },
                                    {
                                        'if': {
                                            'filter_query': '{traffic_light} = 1',
                                            'column_id': "traffic_light"
                                        },
                                        'backgroundColor': 'green',
                                        'color': "white"
                                    }
                                    ])
             ],
            className='full screen', style={'display': 'block', 'height':'450px', 'background-color':'#fbfbfb'}
            )
