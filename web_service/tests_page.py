from front_functions import *

def make_tests_page(logger, color, server_api_address):
	# Тесты
	portfolio_test_field = make_tests_field(request_dqa_portfolio(logger, server_api_address), color)
	issues_test_field = make_tests_field(request_dqa_issues(logger, server_api_address), color)

	return html.Div([
		html.Div([html.H2('Тесты портфеля', style={'text-align':'center'})] + portfolio_test_field,
            className="pretty_portfolio_test_container",
            style={"padding":"5px"}
            ),
            html.Div([ 
                html.H2('Тесты выдач', style={'text-align':'center'})] + issues_test_field,
            className="pretty_issues_test_container",
            style={"padding":"5px"}
            )
        ])