from datetime import datetime
import os
import pandas as pd
import re
import json
from .data_collector import DataCollector

HEALTHY_DATA_FILE_NAME_PATTERN = r'healthy_data_\d{2}-\d{2}-\d{2}.csv'
CURRPORTF_FILE_NAME_PATTERN = r'current_portfolio_\d{2}-\d{2}-\d{2}.json'
PRODUCTS_DYNAMICS_FILE_NAME_PATTERN = r'products_dynamics_\d{2}-\d{2}-\d{2}.csv'
RISK_ESTIMATIONS_FILE_NAME_PATTERN = r'risk_estimations_\d{2}-\d{2}-\d{2}.csv'
RISK_PRODUCTS_FILE_NAME_PATTERN = r'risk_products_\d{2}-\d{2}-\d{2}.csv'
RISK_RATINGS_PRODUCTS_FILE_NAME_PATTERN = r'risk_ratings_products_\d{2}-\d{2}-\d{2}.csv'
ISSUES_FILE_NAME_PATTERN = r'issues_\d{2}-\d{2}-\d{2}.csv'

RISK_PORTFOLIO_METRICS_NAME_PATTERN = r'risk_portfolio_metrics_\d{2}-\d{2}-\d{2}.csv'
RISK_PORTFOLIO_PRODUCTS_NAME_PATTERN = r'risk_portfolio_products_\d{2}-\d{2}-\d{2}.csv'
PORTFOLIO_BALANCES_NAME_PATTERN = r'portfolio_balances_\d{2}-\d{2}-\d{2}.csv'
PORTFOLIO_PRODUCTS_NAME_PATTERN = r'portfolio_products_\d{2}-\d{2}-\d{2}.csv'
ISSUES_PRODUCTS_NAME_PATTERN = r'issues_products_\d{2}-\d{2}-\d{2}.csv'
ISSUES_VINTAGE_NAME_PATTERN = r'issues_vintage_\d{2}-\d{2}-\d{2}.csv'
PORTFOLIO_DR_NAME_PATTERN = r'portfolio_dr_\d{2}-\d{2}-\d{2}.csv'
PORTFOLIO_NPL_NAME_PATTERN = r'portfolio_npl_\d{2}-\d{2}-\d{2}.csv'
DQA_PORTFOLIO_NAME_PATTERN = r'dqa_portfolio_\d{2}-\d{2}-\d{2}.csv'
DQA_ISSUES_NAME_PATTERN = r'dqa_issues_\d{2}-\d{2}-\d{2}.csv'
DQA_COMPARISON_NAME_PATTERN = r'dqa_comparison_\d{2}-\d{2}-\d{2}.csv'
PORTFOLIO_VINTAGES_NAME_PATTERN = r'portfolio_vintages_\d{2}-\d{2}-\d{2}.csv'
PORTFOLIO_HAZARD_NAME_PATTERN = r'portfolio_hazard_\d{2}-\d{2}-\d{2}.csv'

DATE_PATTERN = r'\d{2}-\d{2}-\d{2}' #год-месяц-день, например 22-01-13


class DataProvider():
    def __init__(self, sqlc, distination_path="data/"):
        self.__data_collector = DataCollector(sqlc)
        self.__data_location = distination_path

    def __check_for_local_copy__(self, pattern):
        """Проверяет наличие локальной копии данных"""
        self.log("Проверяю есть ли локальные копии данных")
        _ = self.check_if_file_exists(pattern)
        if _:
            self.log("Локальные копии найдены")
            data_file, data_date = _
        else:
            self.log("Локальные копии не найдены")
            data_file, data_date = None, datetime.now()
        return data_file, data_date

    # враппер над логером, пока просто принт
    def log(self, message):
        print("{0}: {1}".format(datetime.now(), message))

    def clear_cache(self):
        """Чистит закешированные данные"""
        self.log("Начинаю чистить кэш")
        import os
        directory = "data/"

        for f in os.listdir(directory):
            path = os.path.join(directory, f)
            if os.path.isfile(path):
                self.log("Удаляю файл " + path)
                os.remove(path)
        self.log("Закончил чистить кэш")

    def get_available_files(self):
        files_list = sorted(os.listdir(self.__data_location))
        return files_list

    def check_if_file_exists(self, filename_pattern):
        available_files = self.get_available_files()
        print(available_files)
        print("Ищем файл по шаблону ", filename_pattern)
        for af in available_files:
            if re.match(filename_pattern, af):
                date_str = re.search(DATE_PATTERN, af).group(0)
                data_date = datetime.strptime(date_str, "%y-%m-%d")
                data_file = af
                print("Нашли файл:{0}, {1}".format(data_file, data_date))
                return data_file, data_date

    def __reader(self, data_file):
        self.log("Читаю данные из локальных файлов")
        file_path = self.__data_location + data_file
        data = pd.read_csv(file_path)
        return data.to_dict()

    def __saver(self, save_method, data):
        self.log("Сохраняю данные в файл...")
        save_method(data)

    def __uni_saver(self, data_tobe_saved, filename_main_part):
        new_file_name = self.__data_location + filename_main_part + "_{0}.csv".format(datetime.now().strftime("%y-%m-%d"))
        data_tobe_saved.to_csv(new_file_name, index=False)

    def __updater(self, collect_method, filename_main_part):
        self.log("Обновляю данные...")
        data = collect_method()
        self.__uni_saver(data, filename_main_part)
        return data

    def get_risk_portfolio_metrics(self, update):
        self.log("Запрошены риск-метрики по текущему портфелю...")
        # проверяем нет ли локальной копии
        data_file, data_date = self.__check_for_local_copy__(RISK_PORTFOLIO_METRICS_NAME_PATTERN)

        # Пришёл запрос на обновление
        if update:
            data = self.__updater(self.__data_collector.collect_risk_portfolio_metrics, "risk_portfolio_metrics")
            return {"date": data_date, "data": data.to_dict()}
        else:
            if data_file:
                self.log("Читаю данные из локальных файлов")
                return {"date": data_date, "data": self.__reader(data_file)}
            else:
                risk_portfolio_metrics = self.__data_collector.collect_risk_portfolio_metrics()
                self.__uni_saver(risk_portfolio_metrics, "risk_portfolio_metrics")
                return {"date": data_date, "data": risk_portfolio_metrics.to_dict()}

    def get_risk_portfolio_products(self, update):
        self.log("Запрошены риск-метрики по текущему портфелю в разрезе продуктов...")
        # проверяем нет ли локальной копии
        data_file, data_date = self.__check_for_local_copy__(RISK_PORTFOLIO_PRODUCTS_NAME_PATTERN)

        # Пришёл запрос на обновление
        if update:
            data = self.__updater(self.__data_collector.collect_risk_portfolio_products, "risk_portfolio_products")
            return {"date": data_date, "data": data.to_dict()}
        else:
            if data_file:
                return {"date": data_date, "data": self.__reader(data_file)}
            else:
                risk_portfolio_products = self.__data_collector.collect_risk_portfolio_products()
                self.__uni_saver(risk_portfolio_products, "risk_portfolio_products")
                return {"date": data_date, "data": risk_portfolio_products.to_dict()}

    def get_portfolio_balances(self, update):
        self.log("Запрошены балансы по портфелю...")
        # проверяем нет ли локальной копии
        data_file, data_date = self.__check_for_local_copy__(PORTFOLIO_BALANCES_NAME_PATTERN)

        # Пришёл запрос на обновление
        if update:
            data = self.__updater(self.__data_collector.collect_portfolio_balances, "portfolio_balances")
            return {"date": data_date, "data": data.to_dict()}
        else:
            if data_file:
                return {"date": data_date, "data": self.__reader(data_file)}
            else:
                portfolio_balances = self.__data_collector.collect_portfolio_balances()
                self.__uni_saver(portfolio_balances, "portfolio_balances")
                return {"date": data_date, "data": portfolio_balances.to_dict()}

    def get_portfolio_products(self, update):
        self.log("Запрошен продуктовый состав портфеля...")
        # проверяем нет ли локальной копии
        data_file, data_date = self.__check_for_local_copy__(PORTFOLIO_PRODUCTS_NAME_PATTERN)

        # Пришёл запрос на обновление
        if update:
            data = self.__updater(self.__data_collector.collect_portfolio_products, "portfolio_products")
            return {"date": data_date, "data": data.to_dict()}
        else:
            if data_file:
                return {"date": data_date, "data": self.__reader(data_file)}
            else:
                portfolio_products = self.__data_collector.collect_portfolio_products()
                self.__uni_saver(portfolio_products, "portfolio_products")
                return {"date": data_date, "data": portfolio_products.to_dict()}

    def get_issues_products(self, update):
        self.log("Запрошен продуктовый состав выдач...")
        # проверяем нет ли локальной копии
        data_file, data_date = self.__check_for_local_copy__(ISSUES_PRODUCTS_NAME_PATTERN)

        # Пришёл запрос на обновление
        if update:
            data = self.__updater(self.__data_collector.collect_issues_products, "issues_products")
            return {"date": data_date, "data": data.to_dict()}
        else:
            if data_file:
                return {"date": data_date, "data": self.__reader(data_file)}
            else:
                issues_products = self.__data_collector.collect_issues_products()
                self.__uni_saver(issues_products, "issues_products")
                return {"date": data_date, "data": issues_products.to_dict()}

    def get_issues_vintage(self, update):
        self.log("Запрошены винтажи...")
        # проверяем нет ли локальной копии
        data_file, data_date = self.__check_for_local_copy__(ISSUES_VINTAGE_NAME_PATTERN)

        # Пришёл запрос на обновление
        if update:
            data = self.__updater(self.__data_collector.collect_issues_vintage, "issues_vintage")
            return {"date": data_date, "data": data.to_dict()}
        else:
            if data_file:
                return {"date": data_date, "data": self.__reader(data_file)}
            else:
                issues_vintage = self.__data_collector.collect_issues_vintage()
                self.__uni_saver(issues_vintage, "issues_vintage")
                return {"date": data_date, "data": issues_vintage.to_dict()}

    def get_portfolio_npl(self, update):
        self.log("Запрошен NPL по портфелю ...")
        # проверяем нет ли локальной копии
        data_file, data_date = self.__check_for_local_copy__(PORTFOLIO_NPL_NAME_PATTERN)

        # Пришёл запрос на обновление
        if update:
            data = self.__updater(self.__data_collector.collect_portfolio_npl, "portfolio_npl")
            return {"date": data_date, "data": data.to_dict()}
        else:
            if data_file:
                return {"date": data_date, "data": self.__reader(data_file)}
            else:
                portfolio_npl = self.__data_collector.collect_portfolio_npl()
                self.__uni_saver(portfolio_npl, "portfolio_npl")
                return {"date": data_date, "data": portfolio_npl.to_dict()}

    def get_portfolio_dr(self, update):
        self.log("Запрошен DR по портфелю ...")
        # проверяем нет ли локальной копии
        data_file, data_date = self.__check_for_local_copy__(PORTFOLIO_DR_NAME_PATTERN)

        # Пришёл запрос на обновление
        if update:
            data = self.__updater(self.__data_collector.collect_portfolio_dr, "portfolio_dr")
            return {"date": data_date, "data": data.to_dict()}
        else:
            if data_file:
                return {"date": data_date, "data": self.__reader(data_file)}
            else:
                portfolio_dr = self.__data_collector.collect_portfolio_dr()
                self.__uni_saver(portfolio_dr, "portfolio_dr")
                return {"date": data_date, "data": portfolio_dr.to_dict()}
            
    def get_dqa_portfolio(self, update):
        self.log("Запрошены результаты тестов качества данных по портфелю")
        
       
        # проверяем нет ли локальной копии
        data_file, data_date = self.__check_for_local_copy__(DQA_PORTFOLIO_NAME_PATTERN)

        # Пришёл запрос на обновление
        if update:
            data = self.__updater(self.__data_collector.collect_dqa_portfolio, "dqa_portfolio")
            return {"date": data_date, "data": data.to_dict()}
        else:
            if data_file:
                return {"date": data_date, "data": self.__reader(data_file)}
            else:
                dqa_portfolio = self.__data_collector.collect_dqa_portfolio()
                self.__uni_saver(dqa_portfolio, "dqa_portfolio")
                return {"date": data_date, "data": dqa_portfolio.to_dict()}
            
    def get_dqa_issues(self, update):
        self.log("Запрошены результаты тестов качества данных по выдачам")
         
        # проверяем нет ли локальной копии
        data_file, data_date = self.__check_for_local_copy__(DQA_ISSUES_NAME_PATTERN)

        # Пришёл запрос на обновление
        if update:
            data = self.__updater(self.__data_collector.collect_dqa_issues, "dqa_issues")
            return {"date": data_date, "data": data.to_dict()}
        else:
            if data_file:
                return {"date": data_date, "data": self.__reader(data_file)}
            else:
                dqa_issues = self.__data_collector.collect_dqa_issues()
                self.__uni_saver(dqa_issues, "dqa_issues")
                return {"date": data_date, "data": dqa_issues.to_dict()}
        
    def get_dqa_comparison(self, update):
        self.log("Запрошены результаты сравнения предыдущего и текущего состояния портфеля")
        
        # проверяем нет ли локальной копии
        data_file, data_date = self.__check_for_local_copy__(DQA_COMPARISON_NAME_PATTERN)

        # Пришёл запрос на обновление
        if update:
            data = self.__updater(self.__data_collector.collect_dqa_comparison, "dqa_comparison")
            return {"date": data_date, "data": data.to_dict()}
        else:
            if data_file:
                return {"date": data_date, "data": self.__reader(data_file)}
            else:
                dqa_comparison = self.__data_collector.collect_dqa_comparison()
                self.__uni_saver(dqa_comparison, "dqa_comparison")
                return {"date": data_date, "data": dqa_comparison.to_dict()}
            
    def get_mmb_portfolio_vintages(self, update):
        self.log("Запрошены винтажи по портфелю")
        
        # проверяем нет ли локальной копии
        data_file, data_date = self.__check_for_local_copy__(PORTFOLIO_VINTAGES_NAME_PATTERN)

        # Пришёл запрос на обновление
        if update:
            data = self.__updater(self.__data_collector.collect_mmb_portfolio_vintages, "portfolio_vintages")
            return {"date": data_date, "data": data.to_dict()}
        else:
            if data_file:
                return {"date": data_date, "data": self.__reader(data_file)}
            else:
                portfolio_vintages = self.__data_collector.collect_mmb_portfolio_vintages()
                self.__uni_saver(portfolio_vintages, "portfolio_vintages")
                return {"date": data_date, "data": portfolio_vintages.to_dict()}
            
    def get_mmb_portfolio_hazard(self, update):
        self.log("Запрошен hazard rate по портфелю")
        
        # проверяем нет ли локальной копии
        data_file, data_date = self.__check_for_local_copy__(PORTFOLIO_HAZARD_NAME_PATTERN)

        # Пришёл запрос на обновление
        if update:
            data = self.__updater(self.__data_collector.collect_mmb_portfolio_hazard, "portfolio_hazard")
            return {"date": data_date, "data": data.to_dict()}
        else:
            if data_file:
                return {"date": data_date, "data": self.__reader(data_file)}
            else:
                portfolio_hazard = self.__data_collector.collect_mmb_portfolio_hazard()
                self.__uni_saver(portfolio_hazard, "portfolio_hazard")
                return {"date": data_date, "data": portfolio_hazard.to_dict()}
        