import requests


def refresh():
  API_ADDRESS = 'http://10.114.17.86:5001/api/'
  resp = requests.get('http://10.114.17.86:5001/api/clear_cache')
  print(resp.text)
  print('\nDATA REQUESTS\n')
  print('risk_portfolio_metrics updated\n' if requests.get(API_ADDRESS + 'risk/portfolio/metrics').status_code == 200 else 'risk_portfolio_metrics NOT UPDATED')
  print('risk_portfolio_products updated\n' if requests.get(API_ADDRESS + 'risk/portfolio/products').status_code == 200 else 'risk_portfolio_products NOT UPDATED')
  print('portfolio_balances updated\n' if requests.get(API_ADDRESS + 'portfolio/balances').status_code == 200 else 'portfolio_balances NOT UPDATED')
  print('portfolio_products updated\n' if requests.get(API_ADDRESS + 'portfolio/products').status_code == 200 else 'portfolio_products NOT UPDATED')
  print('issues_products updated\n' if requests.get(API_ADDRESS + 'issues/products').status_code == 200 else 'issues_products NOT UPDATED')
  print('portfolio_npl updated\n' if requests.get(API_ADDRESS + 'portfolio/npl').status_code == 200 else 'portfolio_npl NOT UPDATED')
  print('portfolio_dr updated\n' if requests.get(API_ADDRESS + 'portfolio/dr').status_code == 200 else 'portfolio_dr NOT UPDATED')
  print('issues_vintage updated\n' if requests.get(API_ADDRESS + 'issues/vintage').status_code == 200 else 'issues_vintage NOT UPDATED')
  print('dqa_portfolio updated\n' if requests.get(API_ADDRESS + 'dqa/portfolio').status_code == 200 else 'dqa_portfolio NOT UPDATED')
  print('dqa_issues updated\n' if requests.get(API_ADDRESS + 'dqa/issues').status_code == 200 else 'dqa_issues NOT UPDATED')
  return 0

if __name__ == '__main__':
  refresh()
