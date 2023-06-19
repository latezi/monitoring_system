from front_functions import *

def make_tests_tab(logger, server_api_address):
    colors_list = ['#4cb5f5', '#dbae58', '#1f3f49', '#7e909a', '#ea6a47', '#0091d5', '#6ab187', '#488a99', '#1c4e80',
               '#a5d8dd', '#ced2cc', '#d32d41', '#b3c100']
	# Тесты
    dqa_comparisson_df = request_dqa_comparisson(logger, server_api_address)
    data_tests_comparisson_dr_new = Metric_container('Дефолт рейт текущей сборки', dqa_comparisson_df, 'defaults_share_curr')
    data_tests_comparisson_dr_old = Metric_container('Дефолт рейт предыдущей сборки', dqa_comparisson_df, 'defaults_share_prev')
    data_tests_comparisson_dr_new.make_scatter_field('portfolio_tests_dr_scatter_1_1', 'full screen', 'defaults_share_curr', color=colors_list[0])
    data_tests_comparisson_dr_old.make_scatter_field('portfolio_tests_dr_scatter_1_2', 'full screen', 'defaults_share_prev', color=colors_list[1])

    data_tests_comparisson_count_new = Metric_container('Количество наблюдений в текущей сборке', dqa_comparisson_df, 'count_curr')
    data_tests_comparisson_count_old = Metric_container('Количество наблюдений в предыдущей сборке', dqa_comparisson_df, 'count_prev')
    data_tests_comparisson_count_new.make_scatter_field('portfolio_tests_count_scatter_1_1', 'full screen', 'count_curr', color=colors_list[0])
    data_tests_comparisson_count_old.make_scatter_field('portfolio_tests_count_scatter_1_2', 'full screen', 'count_prev', color=colors_list[1])

    dr_figure = data_tests_comparisson_dr_new.scatter_figure
    dr_figure.add_trace(data_tests_comparisson_dr_old.scatter),
    dr_figure.update_layout(title='Сравнение предыдущей сборки витрины мониторинга с текущей. DR', height=350)

    count_figure = data_tests_comparisson_count_new.scatter_figure
    count_figure.add_trace(data_tests_comparisson_count_old.scatter),
    count_figure.update_layout(title='Сравнение предыдущей сборки витрины мониторинга с текущей. Численность', height=350)


    portfolio_test_field = make_tests_agg_field(pd.concat([request_dqa_portfolio(logger, server_api_address), 
                                                           request_dqa_issues(logger, server_api_address)]))

    return html.Div([
            html.Div([
                html.Div([
                    dcc.Graph(
                        figure=dr_figure,
                    ),
                    html.Span('DR – доля КТ, которые выйдут в дефолт в течение года', className='tooltiptext')
                ],
                    id='dr_figure_container',
                    className='pretty_container three columns tooltip',
                ),
                portfolio_test_field,
            ], className='full screen'),
            html.Div([
                html.Div([
                    dcc.Graph(
                        figure=count_figure,
                    ),
                    html.Span('DR – доля КТ, которые выйдут в дефолт в течение года', className='tooltiptext')
                ],
                    id='dr_figure_container',
                    className='pretty_container full screen tooltip',
                ),
            ], className='full screen'),
            ], className='full_screen')



    