from pyspark.sql import functions as f
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType
from datetime import datetime
import datetime as dt
import pandas as pd
import numpy as np
from pyspark import SparkContext, SparkConf, HiveContext
import os
import json
from .dq_tests import get_portfolio_dq_tests, get_issues_dq_tests
from .lubenets_query import amrlirt_query


def null_to_empty_keys(d):
    if 'null' in d.keys():
        d['empty'] = d.pop('null')


class DataCollector:
    def __init__(self, spark_context, distination_path='data/'):

        self.__sc = spark_context
        # запуск hivecontext
        self.__sqlc = HiveContext(self.__sc)

        self.__distination_path = distination_path

        # TODO: читать названия столбцов из файла конфигурации
        self.__portfolio_table_name = 'витрина.таблица1'
        self.__portfolio_prev_table_name = 'витрина.таблица2'
        self.__issues_table_name = 'витрина.таблица3'
        self.__dates_column_name = 'report_dt'
        self.__current_default_flag_name = 'cur_default'
        self.__default_in_1y_flag_name = 'default_in_1y_flag'
        self.__due_debt_name = 'debt_due_rub'
        self.__ovr_debt_name = 'debt_ovr_rub'
        self.__due_interest_name = 'intrst_due_bal_rub'
        self.__ovr_interest_name = 'intrst_ovr_bal_rub'
        self.__ovr_days_count_name = 'delinq_days'
        self.__offbalance_name = 'off_balance_at_report_dt'
        self.__restruct_table_name = 'витрина.таблица4'

    # враппер над логером, пока просто принт (метод не статик, потому что есть план дёргать через него внешний делегат)
    def log(self, message):
        print('{0}: {1}'.format(datetime.now(), message))

    def collect_data(self, save: bool = False) -> dict:
        """
        ЛЕГАСИ? УДАЛИТЬ?
        Метод сбора данных

        @param save: Параметр, надо ли сохранять данные
        @return:
        """
        portfolio_table = self.__sqlc.table(self.__portfolio_table_name)
        # Базовые статистики
        base_stats = self.collect_main_stats(portfolio_table)
        # Данные по здоровым
        healthy_data = self.collect_healthy_data(portfolio_table)

        # Сохранение
        if save:
            self.log('Сохраняю файлы')
            healthy_data.to_json(self.__distination_path + 'healhy_data_{}.json'.format(datetime.now()))
            with open(self.__distination_path + 'base_stats_{}.json'.format(datetime.now()), mode='w') as fs:
                fs.write(json.dumps(base_stats))
            pass
        else:
            return {'base_stats': base_stats,
                    'healthy_data': healthy_data}

    def __collect_risk_portfolio_metrics_products(self, by_products=False):
        #ЛЕГАСИ? УДАЛИТЬ?
        self.log('Собираю данные из базы...')
        portfolio = self.__sqlc.table(self.__portfolio_table_name)

        group_key = ['report_dt']
        if by_products:
            group_key = ['report_dt', 'cred_type']

        portfolio = portfolio.withColumn('el', f.col('pd_pit') * f.col('lgd_monitoring_longrun'))
        healthy_metrics = portfolio \
            .filter(f.col('cur_default') == 0) \
            .groupby(group_key) \
            .agg(f.mean('pd_pit').alias('pd_pit'),
                 f.mean('lgd_monitoring_longrun').alias('lgd_lr'))
        bad_metrics = portfolio \
            .filter(f.col('cur_default') == 1) \
            .groupby(group_key) \
            .agg(f.mean('lgd_indefault_longrun').alias('lgd_id'))
        full_metrics = portfolio \
            .groupby(group_key) \
            .agg(f.mean('el').alias('el'))

        metrics_data = healthy_metrics.join(bad_metrics, on=group_key).join(full_metrics, on=group_key).orderBy(
            group_key)
        metrics_data_pd = metrics_data.toPandas()
        metrics_data_pd['cor'] = 1.0

        return metrics_data_pd

    def collect_risk_portfolio_metrics(self):
        # ЛЕГАСИ? УДАЛИТЬ?
        return self.__collect_risk_portfolio_metrics_products(False)

    def collect_risk_portfolio_products(self):
        # ЛЕГАСИ? УДАЛИТЬ?
        return self.__collect_risk_portfolio_metrics_products(True)

    def collect_portfolio_balances(self) -> pd.DataFrame:
        """
        Собирает информацию по балансовым и внебалансовым остаткам по КТ в портфеле
        @return: Информация по балансовым и внебалансовым остаткам по КТ в портфеле
        """
        self.log('Собираю данные из базы...')
        portfolio = self.__sqlc.table(self.__portfolio_table_name)
        portfolio_balances = portfolio \
            .withColumn('balance',
                        f.col('debt_due_rub') + f.col('debt_ovr_rub') + f.col('intrst_due_bal_rub') + f.col(
                            'intrst_ovr_bal_rub')) \
            .groupBy('report_dt') \
            .agg(f.sum('debt_due_rub').cast(DoubleType()).alias('total_due_debt'),
                 f.sum('debt_ovr_rub').cast(DoubleType()).alias('total_ovr_debt'),
                 f.sum('intrst_due_bal_rub').cast(DoubleType()).alias('total_due_int'),
                 f.sum('intrst_ovr_bal_rub').cast(DoubleType()).alias('total_ovr_int'),
                 f.sum('balance').cast(DoubleType()).alias('total_balance'),
                 f.sum('off_balance_at_report_dt').cast(DoubleType()).alias('total_offbalance')) \
            .orderBy('report_dt')
        portfolio_balances_pd = portfolio_balances.toPandas()
        return portfolio_balances_pd

    def collect_portfolio_products(self) -> pd.DataFrame:
        """
        Собирает количество и сумму КТ в разрезе типов продуктов
        @return: количество и сумма КТ в разрезе типов продуктов
        """
        self.log('Собираю данные из базы...')
        portfolio = self.__sqlc.table(self.__portfolio_table_name)
        portfolio_products = portfolio \
            .withColumn('debt', f.col('debt_due_rub') + f.col('debt_ovr_rub')) \
            .groupBy(['report_dt', 'cred_type']) \
            .agg(f.count('agr_cred_id').alias('cnt'),
                 f.sum('debt').cast(DoubleType()).alias('sum')) \
            .orderBy(['report_dt', 'cred_type'])
        portfolio_products_pd = portfolio_products.toPandas()
        return portfolio_products_pd

    def collect_issues_products(self) -> pd.DataFrame:
        """
        Собирает количество и сумму выдач в разрезе типов продуктов
        @return: количество и сумма выдач в разрезе типов продуктов
        """
        self.log('Собираю данные из базы...')
        issues = self.__sqlc.table(self.__issues_table_name)

        issues_products = issues \
            .filter(f.col('generation') >= '2011-01-01') \
            .withColumn('debt', f.col('debt_due_rub') + f.col('debt_ovr_rub')) \
            .groupBy(['generation', 'cred_type']) \
            .agg(f.count('agr_cred_id').alias('cnt'),
                 f.sum('debt').cast(DoubleType()).alias('sum')) \
            .orderBy(['generation', 'cred_type'])
        issues_products_pd = issues_products.toPandas()
        issues_products_pd['generation'] = pd.to_datetime(issues_products_pd['generation']).apply(str)
        return issues_products_pd

    def collect_issues_vintage(self) -> pd.DataFrame:
        """
        Собирает даныне для винтажного анализа
        @return: Винтажи
        """
        self.log('Собираю данные из базы...')
        portfolio = self.__sqlc.table(self.__portfolio_table_name)

        portfolio = portfolio \
            .filter(f.col('generation') >= '2011-01-01') \
            .withColumn('mob', f.round(f.months_between(f.col('report_dt'), f.col('generation'))))

        vintages = portfolio \
            .groupBy(['generation', 'mob']) \
            .agg(f.mean('cur_default').alias('npl')) \
            .orderBy(['generation', 'mob']) \
            .toPandas()
        return vintages

    def collect_portfolio_npl(self) -> pd.DataFrame:
        """
        Собирает доли в портфеле КТ с просрочками 1+ и 91+ на даты
        @return: Доли в портфеле КТ с просрочками 1+ и 91+ на даты
        """
        self.log('Собираю данные из базы...')
        portfolio = self.__sqlc.table(self.__portfolio_table_name)

        npl_data = portfolio \
            .groupBy('report_dt') \
            .agg(f.mean(f.when(f.col('delinq_days') > 90, 1).otherwise(0)).alias('npl91'),
                 f.mean(f.when(f.col('delinq_days') > 0, 1).otherwise(0)).alias('npl1')) \
            .orderBy('report_dt') \
            .toPandas()
        return npl_data

    def collect_portfolio_dr(self) -> pd.DataFrame:
        """
        Собирает информацию о DR в историю
        @return: Dr на даты в прошлом
        """
        self.log('Собираю данные из базы...')
        portfolio = self.__sqlc.table(self.__portfolio_table_name)

        dr_data = portfolio \
            .filter(f.col('cur_default') == 0) \
            .groupBy('report_dt') \
            .agg(f.mean(f.col('default_in_1y_flag')).alias('dr')) \
            .orderBy('report_dt') \
            .toPandas()

        dr_data = dr_data[:-12]
        return dr_data

    def collect_dqa_portfolio(self) -> pd.DataFrame:
        """
        Собирает результаты тестов на КД по портфелю
        @return: Результаты тестов на КД по портфелю
        """
        self.log('Собираю данные из базы...')
        portfolio = self.__sqlc.table(self.__portfolio_table_name)

        # получаем тесты
        portfolio_qa = get_portfolio_dq_tests(portfolio)

        # раним тесты
        tests_results = portfolio_qa.run_tests(self.log)
        dqa_result = pd.DataFrame(tests_results)

        return dqa_result

    def collect_dqa_issues(self) -> pd.DataFrame:
        """
        Собирает результаты тестов на КД по выдачам
        @return: Результаты тестов на КД по выдачам
        """
        self.log('Собираю данные из базы...')
        issues = self.__sqlc.table(self.__issues_table_name)

        # получаем тесты
        issues_qa = get_issues_dq_tests(issues)

        # раним тесты
        tests_results = issues_qa.run_tests(self.log)
        dqa_result = pd.DataFrame(tests_results)

        return dqa_result

    def collect_dqa_comparison(self) -> pd.DataFrame:
        """
        Собирает агрегаты по предыдущему и текущему состояниям данных
        @return: агрегаты по предыдущему и текущему состояниям данных
        """
        self.log('Собираю данные из базы...')

        portfolio = self.__sqlc.table(self.__portfolio_table_name)
        portfolio_prev = self.__sqlc.table(self.__portfolio_prev_table_name)

        def get_aggregates(dataset, postfix='_curr'):
            full_portfolio = dataset.groupby('report_dt').agg(
                f.count('agr_cred_id').cast(DoubleType()).alias('count' + postfix),
                f.sum('balance_at_report_dt').cast(DoubleType()).alias('balance_sum' + postfix),
                f.sum('off_balance_at_report_dt').cast(DoubleType()).alias('off_balance_sum' + postfix),
                f.mean('cur_default').cast(DoubleType()).alias('defaults_share' + postfix))

            healthy_portfolio = dataset.filter(f.col('cur_default') == 0) \
                .groupby('report_dt') \
                .agg(f.count('default_in_1y_flag').cast(DoubleType()).alias('DR' + postfix))

            aggregates = full_portfolio.join(healthy_portfolio, on=['report_dt'], how='left')
            return aggregates

        cur_aggs = get_aggregates(portfolio)
        prev_aggs = get_aggregates(portfolio_prev, '_prev')

        result = cur_aggs.join(prev_aggs, on='report_dt')
        result_pd = result.toPandas()
        return result_pd
    
    def collect_mmb_portfolio(self):
        """
        Собирает информацию по портфелю, маршрутам, реструктуризациям и категориям товаров для винтажей, хазарт и рол рейту
        @return: Информация по портфелю
        """
        self.log('Собираю данные из базы...')
        
        # backbone
        mmb = self.__sqlc.table(self.__portfolio_table_name)
        portfolio = mmb\
                    .withColumn('report_dt', f.to_date('report_dt'))\
                    .withColumn('signed_dt', f.to_date('signed_dt'))\
                    .filter(f.col('cred_type') != 'Г')\
                    .withColumn('signed_m', f.trunc('signed_dt', 'MM'))\
                    .select('report_dt', 'inn', 'agr_cred_id', 'delinq_days', 'balance_at_report_dt', 'off_balance_at_report_dt', 'cred_type', 'unused_limit_rub',
                            'signed_dt', 'cur_default', 'issue_dt', 'debt_ovr_rub', 'debt_due_rub', 'generation', 'eks_src_id', 'product', 'signed_m')\
                    .withColumn('mob', f.floor(f.months_between(f.col("report_dt"), f.col("signed_dt"))))
        
        # first rows for denom of vintages
        issues_clear_mmb = self.__sqlc.table(self.__issues_table_name)
        issues_clear = issues_clear_mmb\
                    .filter(f.col('signed_dt') >= '2021-01-01')\
                    .filter(f.col('cred_type') != 'Г')\
                    .select(f.col('agr_cred_id').alias("clear_agr_cred"), 'unused_limit_rub', 'debt_ovr_rub', 'debt_due_rub')\
                    .withColumn("gen_summ", f.col('unused_limit_rub') + f.col('debt_ovr_rub') + f.col('debt_due_rub')).drop('unused_limit_rub').drop('debt_ovr_rub').drop('debt_due_rub')
        
        portfolio = portfolio.join(issues_clear, on=[portfolio.agr_cred_id == issues_clear.clear_agr_cred], how="left")
        
        # amrlirt_query
        cols_from_lirt = ['TotalValue', 'req_id', 'req_date', 'inn']
        lirt = self.__sqlc.sql(amrlirt_query).select(cols_from_lirt).distinct()
        
        # routes table
        win = Window.partitionBy('b.inn', 'agr_cred_id').orderBy(f.col('req_date').desc())

        routes = portfolio.alias("a").join(
            lirt.alias("b"),
            on=[
                f.col("a.inn") == f.col("b.inn"),
                f.to_date(f.col('req_date')) <= f.col('signed_m'),
                f.add_months(f.col('signed_m'), -6) <= f.to_date(f.col('req_date'))
            ],
            how='left'
        )\
        .withColumn('rn', f.row_number().over(win))\
        .filter(f.col('rn') == 1)\
        .select("rn", f.col("agr_cred_id").alias("agr_cred"), 'req_date')
        
        # routes join
        portfolio = portfolio.alias("a").join(
            routes.alias("b"),
            on=[
                f.col("agr_cred_id") == f.col("agr_cred"),
                f.to_date(f.col('req_date')) <= f.col('a.signed_m'),
                f.add_months(f.col('a.signed_m'), -6) <= f.to_date(f.col('req_date'))
            ],
            how='left'
        )\
        .withColumn('route', f.when(f.col('rn') == 1, 1).otherwise(0))\
        .drop('rn', "agr_cred")
        
        # categories of credits
        directory = "local_data/"
        path = ""
        for name in os.listdir(directory):
            if name == "Список_продуктов_дополненный_.csv":
                path = os.path.join(directory, name)
        
        # обработка случая с ненайденным файлом
        if path == "":
            return pd.DataFrame()
        
        df = pd.read_csv(path, sep=";", encoding="cp1251")
        df.columns = ["Product", "Category"]
        df = df.set_index("Product")["Category"].to_dict()

        # restructuring
        restruct = self.__sqlc.table(self.__restruct_table_name)\
                .select("id", 'default_in_dt')\
                .withColumn('c_date_beg', f.last_day(f.to_date('default_in_dt'))).drop('default_in_dt')\
                .filter(f.col("c_date_beg") >= "2021-01-01").dropDuplicates()\
                .withColumn('restruct', f.lit(1))\
                .withColumn("rn", f.row_number().over(Window.partitionBy("id").orderBy("c_date_beg"))).filter(f.col("rn") == 1).drop("rn")
        
        # wrapping function for categories
        def translate(mapping):
            def translate_(col):
                return mapping.get(col)
            return f.udf(translate_, StringType())
        
        data_container = portfolio.join(restruct, on = [portfolio.eks_src_id == restruct.id], how='left')\
                          .withColumn('mob', f.ceil(f.months_between(f.col("report_dt"), f.col("signed_dt"))))\
                          .withColumn('summ', f.col("debt_ovr_rub") + f.col("debt_due_rub") + f.col("unused_limit_rub"))\
                          .withColumn('IP', f.when( f.length('inn') == 12, 1).otherwise(0))\
                          .withColumn('category', translate(df)(f.col("product")))\
                          .withColumn("balance", f.coalesce(f.col("balance_at_report_dt"), f.lit(0)) + f.coalesce(f.col("off_balance_at_report_dt"), f.lit(0)))\
                          .withColumn('dpd1', f.when(0 < f.col('delinq_days'), 1).otherwise(0))\
                          .withColumn('dpd30', f.when(29 < f.col('delinq_days'), 1).otherwise(0))\
                          .withColumn('dpd60', f.when(59 < f.col('delinq_days'), 1).otherwise(0))\
                          .withColumn('dpd90', f.when(90 < f.col('delinq_days'), 1).otherwise(0))
        
        return data_container
    
    def encode_categories(self, df):
        """
        Кодирует категории в тип int для сокращения памяти
        @return: Информация по портфелю
        """
        if "category" in df.columns:
            dic = {
                "1221+прочее": 1,
                "Карта": 2,
                "Оборотный": 3,
                "Инвестиционный": 4,
                "DEGL": 5,
                "Овердрафт": 6,
                "Риск на актив": 7,
                "Гарантия": 8,
                "ФЛ": 9,
                "КСБ": 10,
                None: 11
            }
            df["category"] = df["category"].replace(dic)
        return df
    
    def collect_mmb_portfolio_vintages(self) -> pd.DataFrame:
        """
        Группирует данные по винтажам
        @return: Данные по винтажам
        """
        data_container = self.collect_mmb_portfolio().filter(f.col('signed_dt') >= '2021-01-01')
        
        generations = data_container.groupby("generation", "mob", "IP", "restruct", "category", "route")\
                            .agg(f.count("clear_agr_cred").cast(DoubleType()).alias("gen_count"),\
                                 f.sum("gen_summ").cast(DoubleType()).alias("gen_summ"),\
                                 f.sum(f.when((f.col("dpd1") == 1), 1).otherwise(0)).cast(DoubleType()).alias('npl_1_count'),\
                                 f.sum(f.when((f.col("dpd30") == 1), 1).otherwise(0)).cast(DoubleType()).alias('npl_30_count'),\
                                 f.sum(f.when((f.col("dpd60") == 1), 1).otherwise(0)).cast(DoubleType()).alias('npl_60_count'),\
                                 f.sum(f.when((f.col("dpd90") == 1), 1).otherwise(0)).cast(DoubleType()).alias('npl_90_count'),\
                                 f.sum(f.when((f.col("dpd1") == 1), f.col("summ")).otherwise(0)).cast(DoubleType()).alias('npl_1_summ'),\
                                 f.sum(f.when((f.col("dpd30") == 1), f.col("summ")).otherwise(0)).cast(DoubleType()).alias('npl_30_summ'),\
                                 f.sum(f.when((f.col("dpd60") == 1), f.col("summ")).otherwise(0)).cast(DoubleType()).alias('npl_60_summ'),\
                                 f.sum(f.when((f.col("dpd90") == 1), f.col("summ")).otherwise(0)).cast(DoubleType()).alias('npl_90_summ'))

        generations = generations.toPandas()
        # Оптимизация по памяти
        generations = self.encode_categories(generations)

        return generations

    def collect_mmb_portfolio_hazard(self) -> pd.DataFrame:
        """
        Группирует данные по рол и хазард рейту
        @return: Данные по хазард и рол рейту
        """
        data_container = self.collect_mmb_portfolio()

        # Считаем знаменатель
        denom = data_container.filter((f.col("cur_default") == 0) & (f.col("delinq_days") < 1))\
                              .select("a.agr_cred_id", "a.report_dt", "IP", "restruct", "category", "route", f.col("balance").alias("balance_denom"))

        # Считаем числители
        rol_1_1 = data_container.filter((f.col("delinq_days") > 0) & (f.col("delinq_days") < 31)).select("a.agr_cred_id",  "a.report_dt", "balance")
        hazard_30_2 = data_container.filter((f.col("delinq_days") > 30) & (f.col("delinq_days") < 61)).select("a.agr_cred_id", "a.report_dt", "balance")
        hazard_60_3 = data_container.filter((f.col("delinq_days") > 60) & (f.col("delinq_days") < 91)).select("a.agr_cred_id", "a.report_dt", "balance")
        hazard_90_4 = data_container.filter(f.col("delinq_days") > 90).select("a.agr_cred_id", "a.report_dt", "balance")

        # Объединяем числитель и знаменатель
        denom = data_container.filter((f.col("cur_default") == 0) & (f.col("delinq_days") < 1))\
                              .select("a.agr_cred_id", "a.report_dt", "IP", "restruct", "category", "route", f.col("balance").alias("balance_denom"))

        rol_1_1 = data_container.filter((f.col("delinq_days") > 0) & (f.col("delinq_days") < 31))\
                                .select("a.agr_cred_id", "a.report_dt", f.col("balance").alias("rol_1_1"))

        hazard_30_2 = data_container.filter((f.col("delinq_days") > 30) & (f.col("delinq_days") < 61))\
                                    .select("a.agr_cred_id", "a.report_dt", f.col("balance").alias("hazard_30_2"))

        hazard_60_3 = data_container.filter((f.col("delinq_days") > 60) & (f.col("delinq_days") < 91))\
                                    .select("a.agr_cred_id", "a.report_dt", f.col("balance").alias("hazard_60_3"))

        hazard_90_4 = data_container.filter(f.col("delinq_days") > 90)\
                                    .select("a.agr_cred_id", "a.report_dt", f.col("balance").alias("hazard_90_4"))

        # Объединяем все в одну таблицу (сделано для эффективности по памяти)
        rates = denom.alias("c").join(rol_1_1.alias("d"), on=[f.col("c.agr_cred_id") == f.col("d.agr_cred_id"),\
                                       (f.month(f.col("c.report_dt")) + 12 * f.year(f.col("c.report_dt"))) == \
                                       (f.month(f.col("d.report_dt")) + 12 * f.year(f.col("d.report_dt")) - 1)], how="left")\
                     .drop(f.col("d.agr_cred_id")).drop(f.col("d.report_dt"))

        rates = rates.alias("e").join(hazard_30_2.alias("f"), on=[f.col("e.agr_cred_id") == f.col("f.agr_cred_id"),\
                                       (f.month(f.col("e.report_dt")) + 12 * f.year(f.col("e.report_dt"))) == \
                                       (f.month(f.col("f.report_dt")) + 12 * f.year(f.col("f.report_dt")) - 2)], how="left")\
                     .drop(f.col("f.agr_cred_id")).drop(f.col("f.report_dt"))

        rates = rates.alias("g").join(hazard_60_3.alias("k"), on=[(f.col("g.agr_cred_id") == f.col("k.agr_cred_id")),\
                                       (f.month(f.col("g.report_dt")) + 12 * f.year(f.col("g.report_dt"))) == \
                                       (f.month(f.col("k.report_dt")) + 12 * f.year(f.col("k.report_dt")) - 3)], how="left")\
                     .drop(f.col("k.agr_cred_id")).drop(f.col("k.report_dt"))

        rates = rates.alias("q").join(hazard_90_4.alias("w"), on=[(f.col("q.agr_cred_id") == f.col("w.agr_cred_id")),\
                                       (f.month(f.col("q.report_dt")) + 12 * f.year(f.col("q.report_dt"))) == \
                                       (f.month(f.col("w.report_dt")) + 12 * f.year(f.col("w.report_dt")) - 4)], how="left")\
                     .drop(f.col("w.agr_cred_id")).drop(f.col("w.report_dt")).drop(f.col("q.agr_cred_id"))
        
        rates = rates.groupby("report_dt", "IP", "restruct", "category", "route")\
                     .agg(f.sum(f.col("balance_denom")).cast(DoubleType()).alias('balance_denom'),\
                          f.sum(f.col("rol_1_1")).cast(DoubleType()).alias('rol_1_1'),\
                          f.sum(f.col("hazard_30_2")).cast(DoubleType()).alias('hazard_30_2'),\
                          f.sum(f.col("hazard_60_3")).cast(DoubleType()).alias('hazard_60_3'),\
                          f.sum(f.col("hazard_90_4")).cast(DoubleType()).alias('hazard_90_4'))

        rates = rates.toPandas()
        # Оптимизация по памяти
        rates = self.encode_categories(rates)

        return rates