from front_functions import *

def make_monitoring_tab(logger, server_api_address):
    # Запрос данных по моделям
    dic = {
        "model": ["RISK_LGD_SME_2020_id59009_v1.1.5", "RISK_LGD_SME_2020_id59009_v1.1.5", "RISK_LGD_SME_2020_id59009_v1.1.5"],
        "method": ["PowerStat", "Отклонение фактического LGD от прогнозного LGD", "Отклонение фактического LGD от прогнозного LGD"],
        "traffic_light": [2, 1, 3],
        "meaning": ["16.73", "-0.16856", "-0.09255"],
        "level": ["Основной", "Индикативный", "Основной"],
    }
    df = pd.DataFrame(dic)
    table = make_monitoring_table_field(df)
    
    return html.Div([
        table
    ], className='full screen')
