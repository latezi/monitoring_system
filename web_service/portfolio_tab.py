from front_functions import *

def make_portfolio_tab(logger, server_api_address):

	# Вкладка портфель

# # Сохранение данных в объекты
    data_portfolio_balances = request_portfolio_balances(logger, server_api_address)
    data_portfolio_npl = request_portfolio_npl(logger, server_api_address)
    data_portfolio_dr = request_portfolio_dr(logger, server_api_address)
    data_portfolio_products = request_portfolio_products(logger, server_api_address)

    metric_due_debt = Metric_container('Срочный долг', data_portfolio_balances, 'total_due_debt',value_type='bil') 
    metric_ovr_debt = Metric_container('Просроченный долг', data_portfolio_balances, 'total_ovr_debt',
                                       good_ascending=False, value_type='bil')
    metric_due_intrst = Metric_container('Срочные проценты', data_portfolio_balances, 'total_due_int',
                                         value_type='bil')
    metric_ovr_intrst = Metric_container('Просроченые проценты', data_portfolio_balances, 'total_ovr_int',
                                         good_ascending=False, value_type='bil')
    metric_total_balance = Metric_container('Баланс', data_portfolio_balances, 'total_balance',
                                            value_type='bil')
    metric_total_offbalance = Metric_container('Внебаланс', data_portfolio_balances, 'total_offbalance',
                                               value_type='bil')
    metric_portfolio_npl1 = Metric_container('Просрочка 1+', data_portfolio_npl, 'npl1')
    metric_portfolio_npl91 = Metric_container('Просрочка 90+', data_portfolio_npl, 'npl91')
    metric_portfolio_dr = Metric_container('Default Rate', data_portfolio_dr, 'dr')

	# Создание объектов плашек с метриками

    metric_due_debt.make_metric_field('field_1_1', 'metrics_container pretty_container')
    metric_ovr_debt.make_metric_field('field_1_2', 'metrics_container pretty_container')
    metric_due_intrst.make_metric_field('field_1_3', 'metrics_container pretty_container')
    metric_ovr_intrst.make_metric_field('field_2_1', 'metrics_container pretty_container')
    metric_total_balance.make_metric_field('field_2_2', 'metrics_container pretty_container')
    metric_total_offbalance.make_metric_field('field_2_3', 'metrics_container pretty_container tooltip', 
        'Во внебаланс входят все внебалансы по имеющимся договорам: неиспользованные лимиты по линиям, гарантии и прочее')

    metric_due_debt.make_scatter_field('portfolio_balance_scatter_1_1', 'metrics_container two columns', 'Срочный долг',
                                       color=colors_list[0])
    metric_ovr_debt.make_scatter_field('portfolio_balance_scatter_1_2', 'metrics_container two columns',
                                       'Просроченный долг', color=colors_list[1])
    metric_due_intrst.make_scatter_field('portfolio_balance_scatter_1_3', 'metrics_container two columns',
                                         'Срочные проценты', color=colors_list[2])
    metric_ovr_intrst.make_scatter_field('portfolio_balance_scatter_1_4', 'metrics_container two columns',
                                         'Просроченные проценты', color=colors_list[3])
    metric_total_balance.make_scatter_field('portfolio_balance_scatter_1_5', 'metrics_container two columns', 'Баланс',
                                            color=colors_list[4])
    metric_total_offbalance.make_scatter_field('portfolio_balance_scatter_1_6', 'metrics_container two columns',
                                               'Внебаланс', color=colors_list[5])

    balance_figure = metric_due_debt.scatter_figure
    balance_figure.add_trace(metric_ovr_debt.scatter),
    balance_figure.add_trace(metric_due_intrst.scatter),
    balance_figure.add_trace(metric_ovr_intrst.scatter),
    balance_figure.add_trace(metric_total_balance.scatter),
    balance_figure.add_trace(metric_total_offbalance.scatter),
    balance_figure.update_layout(title='Баланс', height=300)

    metric_portfolio_npl1.make_scatter_field('portfolio_npl_scatter_1_1', 'full screen', 'npl1', color=colors_list[0])
    metric_portfolio_npl91.make_scatter_field('portfolio_npl_scatter_1_2', 'full screen', 'npl91', color=colors_list[1])

    npl_figure = metric_portfolio_npl1.scatter_figure
    npl_figure.add_trace(metric_portfolio_npl91.scatter),
    npl_figure.update_layout(title='NPL', height=400)

    metric_portfolio_dr.make_scatter_field('portfolio_dr_scatter_1_6', 'full screen', 'Default Rate', color=colors_list[0])
    dr_figure = metric_portfolio_dr.scatter_figure
    dr_figure.update_layout(title='Default rate', height=400)

    count_of_products = Divided_by_param_figures(data_portfolio_products, 'cred_type', 'cnt')
    count_of_products.make_scatter_figure('Количество КТ в портфеле в разрезе продуктов, шт', 'one', colors_list=colors_list)
    count_of_products.make_pie_figure('Доля продуктов в портфеле на сегодня')

    sum_of_products = Divided_by_param_figures(data_portfolio_products, 'cred_type', 'sum')
    sum_of_products.make_scatter_figure('Сумма баланса по продуктам в портфеле', 'one', colors_list=colors_list)
    sum_of_products.make_pie_figure('Сумма баланса по продуктам в портфеле на сегодня')

    return html.Div([
            html.Div([
                html.Div([
                    metric_due_debt.metric_field_dash,
                    metric_ovr_debt.metric_field_dash,
                    metric_due_intrst.metric_field_dash,
                    metric_ovr_intrst.metric_field_dash,
                    metric_total_balance.metric_field_dash,
                    metric_total_offbalance.metric_field_dash,
                ],
                    id='metrics_field',
                    className='pretty_container three columns',
                    style={'height': '300px'}
                ),
                html.Div([
                    dcc.Graph(
                        figure=balance_figure,
                        className='pretty_container two columns'
                    )
                ]),
            ],
                className='full screen',
                style={
                    'float': 'left',
                    'text-align': 'center',
                    'height': '320px'
                }
            ),
            html.Div([
                html.Div([
                    dcc.Graph(
                        id='count_of_products_scatter_figure',
                        figure=count_of_products.scatter_fig,
                    )
                ],
                    id='count_of_products_scatter_figure_container',
                    className="pretty_container three columns",
                ),
                html.Div([
                    dcc.Graph(
                        id='count_of_products_pie_figure',
                        figure=count_of_products.pie_fig,
                    )
                ],
                    id='count_of_products_pie_figure_container',
                    className="pretty_container two columns",
                ),
            ],
                className='full screen',
                style={
                    'height': '400px'
                }),
            html.Div([
                html.Div([
                    dcc.Graph(
                        id='sum_of_products_scatter_figure',
                        figure=sum_of_products.scatter_fig,
                    )
                ],
                    id='sum_of_products_scatter_figure_container',
                    className="pretty_container three columns",
                ),
                html.Div([
                    dcc.Graph(
                        id='sum_of_products_pie_figure',
                        figure=sum_of_products.pie_fig,
                    )
                ],
                    id='sum_of_products_pie_figure_container',
                    className="pretty_container two columns",
                ),
            ],
                className='full screen',
                style={
                    'height': '400px'
                }),
            html.Div([
                html.Div([
                    dcc.Graph(
                        figure=npl_figure,
                    ),
                    html.Span('Npl1 это доля КТ с просрочкой 1 и более дней', className='tooltiptext')
                ],
                    id='npl_figure_container',
                    className='pretty_container three columns tooltip',
                ),
                html.Div([
                    dcc.Graph(
                        figure=dr_figure,
                    ),
                    html.Span('DR – доля КТ, которые выйдут в дефолт в течение года', className='tooltiptext')
                ],
                    id='dr_figure_container',
                    className='pretty_container two columns tooltip',
                ),
            ],
                className='full screen',
                style={
                    'height': '400px'
                }),
        ])