import re
from datetime import datetime
from pyspark.sql import functions as f
from pyspark.sql import Window
from .quality_assessor import QualityAssessor, QATest


def get_portfolio_dq_tests(portfolio):
    portfolio_qa = QualityAssessor()
    # Добавляем тесты в ассессор
    qt1 = QATest(name="Просрочка 90+ без флага дефолта",
                 description="Обнаружено {0} наблюдений с просрочкой 90+ без флага дефолта",
                 request=lambda table: table.filter((f.col("cur_default") == 0) & (f.col("delinq_days") > 90)).count(),
                 conditions=lambda x: "red" if x > 0 else "green",
                 table=portfolio)
    portfolio_qa.add_test(qt1)

    qt2 = QATest(name="Наблюдения с флагом дефолта, но без причины:",
                 description="Обнаружено {0} наблюдений с флагом дефолта, но без причины",
                 request=lambda table: table.filter(f.col("cur_default") == 1) \
                 .filter(f.length(f.col("default_reason")) == 0) \
                 .count(),
                 conditions=lambda x: "red" if x > 0 else "green",
                 table=portfolio)
    portfolio_qa.add_test(qt2)

    qt3 = QATest(name="Наблюдения с дефолтом в течение года, но без соответсвующего флага",
                 description="Обнаружено {0} наблюдений с дефолтом в течение года, но без соответсвующего флага",
                 request=lambda table: table.filter(f.col("cur_default") == 0).alias("a") \
                 .join(table.filter(f.col("cur_default") == 1).alias("b"),
                       on=["agr_cred_id"], how="inner") \
                 .filter((f.months_between("b.report_dt", "a.report_dt") <= 12) &
                         (f.months_between("b.report_dt", "a.report_dt") > 0)) \
                 .filter(f.col("a.default_in_1y_flag") == 0).count(),
                 conditions=lambda x: "red" if x > 0 else "green",
                 table=portfolio)

    portfolio_qa.add_test(qt3)
    qt4 = QATest(name="90+ без флага дефолта по клиенту",
                 description="Обнаружено {0} наблюдений с 90+ без флага дефолта по клиенту",
                 request=lambda table: table.filter(f.col("cur_default") == 0) \
                 .filter(f.col("cl_cur_max_delinq") > 90) \
                 .count(),
                 conditions=lambda x: "red" if x > 0 else "green",
                 table=portfolio)
    portfolio_qa.add_test(qt4)

    qt5 = QATest(name="Неуникальные записи",
                 description="Обнаружено {0} задублированный ключей report_dt+agr_cred_id",
                 request=lambda table: table.groupby("report_dt", "agr_cred_id").count() \
                 .filter(f.col("count") > 1) \
                 .count(),
                 conditions=lambda x: "red" if x > 0 else "green",
                 table=portfolio)
    portfolio_qa.add_test(qt5)

    qt6 = QATest(name="Наблюдения с pd ttc вне пределов интервала от 0 до 1:",
                 description="Обнаружено {0} наблюдений с pd ttc вне пределов интервала от 0 до 1",
                 request=lambda table: table.filter(
                     (f.col('pd_ttc') > 1) | (f.col('pd_ttc') < 0) | (f.col('pd_pit') < 0) | (
                             f.col('pd_pit') > 1)).count(),
                 conditions=lambda x: "red" if x > 0 else "green",
                 table=portfolio)
    portfolio_qa.add_test(qt6)

    qt7 = QATest(name="Наблюдения с pd ttc или pd pit равным 1 и флагом дефолта равным 0 либо наоборот:",
                 description=("Обнаружено {0} наблюдений с pd ttc или pd pit равным 1 и флагом дефолта равным" +
                              "0 либо наоборот"),
                 request=lambda table: table.filter(
                     (((f.col('pd_ttc') == 1) | (f.col('pd_pit') == 1)) & (f.col('cur_default') == 0)) |
                     (((f.col('pd_ttc') != 1) | (f.col('pd_pit') != 1)) & (f.col('cur_default') == 1))).count(),
                 conditions=lambda x: "red" if x > 0 else "green",
                 table=portfolio)
    portfolio_qa.add_test(qt7)

    qt8 = QATest(name="Наблюдения с фактическими lgd will default за 12 месяцев до момента дефолта:",
                 description=("Обнаружено {0} наблюдений с фактическими lgd will default за 12 месяцев" +
                              " до момента дефолта"),
                 request=lambda table: table.withColumn('value',
                                                        f.when((f.col('lgd_act').isNotNull()), 1).otherwise(0)).groupby(
                     ['agr_cred_id', 'default_in_1y_dt']) \
                 .agg(f.sum('value').alias('lgd_act_sum')).filter(f.col('lgd_act_sum') > 12).count(),
                 conditions=lambda x: "yellow" if x > 0 else "green",
                 table=portfolio)
    portfolio_qa.add_test(qt8)

    qt9 = QATest(
        name="Наблюдения с незаполненными фактическими lgd will default и заполненной датой дефолта за 12 месяцев:",
        description=("Обнаружено {0} наблюдений с незаполненными фактическими lgd will default и" +
                     " заполненной датой дефолта за 12 месяцев"),
        request=lambda table: table.filter(
            (f.col('lgd_act').isNull()) & (f.col('default_in_1y_dt').isNotNull())).count(),
        conditions=lambda x: "yellow" if x > 0 else "green",
        table=portfolio)
    portfolio_qa.add_test(qt9)

    qt10 = QATest(name="Наблюдения с заполненными фактическими lgd will default и флагом дефолта равным 1:",
                  description=("Обнаружено {0} наблюдений с заполненными фактическими lgd will default" +
                               " и флагом дефолта равным 1"),
                  request=lambda table: table.filter(
                      (f.col('lgd_act').isNotNull()) & (f.col('cur_default') == 1)).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt10)

    qt11 = QATest(name="Наблюдения с фактическими lgd will default вне пределов интервала от 0 до 1:",
                  description=("Обнаружено {0} наблюдений с фактическими lgd will default вне пределов" +
                               " интервала от 0 до 1"),
                  request=lambda table: table.filter((f.col('lgd_act') < 0) & (f.col('lgd_act') > 1)).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt11)

    qt12 = QATest(name="Наблюдения с фактическими lgd in default вне пределов интервала от 0 до 1:",
                  description=("Обнаружено {0} наблюдений с фактическими lgd in default вне пределов интервала" +
                               " от 0 до 1"),
                  request=lambda table: table.filter(
                      (f.col('lgd_indefault_act') < 0) & (f.col('lgd_indefault_act') > 1)).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt12)

    qt13 = QATest(name="Наблюдения с заполненными фактическими lgd in default и флагом дефолта равным 0 либо наоборот:",
                  description=("Обнаружено {0} наблюдений с заполненными фактическими lgd in default и флагом дефолта" +
                               " равным 0 либо наоборот"),
                  request=lambda table: table.filter(
                      ((f.col('lgd_indefault_act').isNotNull()) & (f.col('cur_default') == 0)) |
                      ((f.col('lgd_indefault_act').isNull()) & (f.col('cur_default') == 1))).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt13)

    qt14 = QATest(name="Наблюдения с заполненными фактическими ccf за 12 месяцев до дефолта:",
                  description="Обнаружено {0} наблюдений с заполненными фактическими ccf за 12 месяцев до дефолта",
                  request=lambda table: table.withColumn('value', f.when((f.col('ccf_act').isNotNull()), 1).otherwise(
                      0)).groupby(['agr_cred_id', 'default_in_1y_dt'])\
                  .agg(f.sum('value').alias('ccf_act_sum')).filter(f.col('ccf_act_sum') > 12).count(),
                  conditions=lambda x: "yellow" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt14)

    qt15 = QATest(
        name=("Наблюдения с незаполненными фактическими ccf и заполненной датой дефолта за 12 месяцев" +
              " для внебалансовых продуктов:"),
        description=("Обнаружено {0} наблюдений с незаполненными фактическими ccf и заполненной датой дефолта за 12" +
                     " месяцев для внебалансовых продуктов"),
        request=lambda table: table.filter((f.col('ccf_act').isNull()) & (f.col('default_in_1y_dt').isNotNull())
                                           & (f.col('cred_type').isin(['НКЛ', 'ОВ']))).count(),
        conditions=lambda x: "yellow" if x > 0 else "green",
        table=portfolio)
    portfolio_qa.add_test(qt15)

    qt16 = QATest(name="Наблюдения с заполненными фактическими ccf и флагом дефолта равным 1:",
                  description="Обнаружено {0} наблюдений с заполненными фактическими ccf и флагом дефолта равным 1",
                  request=lambda table: table.filter(
                      (f.col('ccf_act').isNotNull()) & (f.col('cur_default') == 1)).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt16)

    qt17 = QATest(name="Наблюдения с модельными lgd will default вне пределов интервала от 0 до 1:",
                  description=("Обнаружено {0} наблюдений с модельными lgd will default вне пределов" +
                               " интервала от 0 до 1"),
                  request=lambda table: table.filter(
                      (f.col('lgd_monitoring_longrun') < 0) & (f.col('lgd_monitoring_longrun') > 1)).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt17)

    qt18 = QATest(
        name="Наблюдения с незаполненными модельными lgd will default и флагом дефолта равным 0 либо наоборот:",
        description=("Обнаружено {0} наблюдений с незаполненными модельными lgd will default и" +
                     " флагом дефолта равным 0 либо наоборот"),
        request=lambda table: table.filter(((f.col('lgd_monitoring_longrun').isNull()) & (f.col('cur_default') == 0)) |
                                           ((f.col('lgd_monitoring_longrun').isNotNull()) & (
                                                   f.col('cur_default') == 1))).count(),
        conditions=lambda x: "red" if x > 0 else "green",
        table=portfolio)
    portfolio_qa.add_test(qt18)

    qt19 = QATest(name="Наблюдения с модельными lgd will default с эффектом downturn вне пределов интервала от 0 до 1:",
                  description=("Обнаружено {0} наблюдений с модельными lgd will" +
                               " default с эффектом downturn вне пределов интервала от 0 до 1"),
                  request=lambda table: table.filter(
                      (f.col('lgd_monitoring_downturn') < 0) & (f.col('lgd_monitoring_downturn') > 1)).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt19)

    qt20 = QATest(
        name=("Наблюдения с незаполненными модельными lgd will default с эффектом downturn" +
              " и флагом дефолта равным 0 или наоборот:"),
        description=("Обнаружено {0} наблюдений с незаполненными модельными lgd will default" +
                     " с эффектом downturn и флагом дефолта равным 0 или наоборот"),
        request=lambda table: table.filter(((f.col('lgd_monitoring_downturn').isNull()) & (f.col('cur_default') == 0)) |
                                           ((f.col('lgd_monitoring_downturn').isNotNull()) & (
                                                   f.col('cur_default') == 1))).count(),
        conditions=lambda x: "red" if x > 0 else "green",
        table=portfolio)
    portfolio_qa.add_test(qt20)

    qt21 = QATest(
        name="Наблюдения с модельными lgd will default с эффектом downturn меньше, чем модельные lgd will default:",
        description=("Обнаружено {0} наблюдений с модельными lgd will default с" +
                     " эффектом downturn меньше, чем модельные lgd will default"),
        request=lambda table: table.filter(
            (f.col('lgd_monitoring_downturn') < f.col('lgd_monitoring_longrun'))).count(),
        conditions=lambda x: "red" if x > 0 else "green",
        table=portfolio)
    portfolio_qa.add_test(qt21)

    qt22 = QATest(name="Наблюдения с незаполненными модельными lgd in default и флагом дефолта равным 1 либо наоборот:",
                  description=("Обнаружено {0} наблюдений с незаполненными модельными lgd in default" +
                               " и флагом дефолта равным 1 либо наоборот"),
                  request=lambda table: table.filter(
                      ((f.col('lgd_indefault_longrun').isNull()) & (f.col('cur_default') == 1)) |
                      ((f.col('lgd_indefault_longrun').isNotNull()) & (f.col('cur_default') == 0))).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt22)

    qt23 = QATest(name="Наблюдения с модельными lgd in default с эффектом downturn вне пределов интервала от 0 до 1:",
                  description=("Обнаружено {0} наблюдений с модельными lgd in default с эффектом downturn" +
                               " вне пределов интервала от 0 до 1"),
                  request=lambda table: table.filter(
                      (f.col('lgd_indefault_downturn') < 0) & (f.col('lgd_indefault_downturn') > 1)).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt23)

    qt24 = QATest(
        name=("Наблюдения с незаполненными модельными lgd in default с эффектом downturn" +
              " и флагом дефолта равным 1 либо наоборот:"),
        description=("Обнаружено {0} наблюдений с незаполненными модельными lgd in default с эффектом downturn" +
                     " и флагом дефолта равным 1 либо наоборот"),
        request=lambda table: table.filter(((f.col('lgd_indefault_downturn').isNull()) & (f.col('cur_default') == 1)) |
                                           ((f.col('lgd_indefault_downturn').isNotNull()) & (
                                                   f.col('cur_default') == 0))).count(),
        conditions=lambda x: "red" if x > 0 else "green",
        table=portfolio)
    portfolio_qa.add_test(qt24)

    qt25 = QATest(
        name="Наблюдения с модельными lgd in default с эффектом downturn меньше, чем модельные lgd in default:",
        description=("Обнаружено {0} наблюдений с модельными lgd in default с эффектом downturn меньше," +
                     " чем модельные lgd in default"),
        request=lambda table: table.filter((f.col('lgd_indefault_downturn') < f.col('lgd_indefault_longrun'))).count(),
        conditions=lambda x: "red" if x > 0 else "green",
        table=portfolio)
    portfolio_qa.add_test(qt25)

    qt26 = QATest(name="Наблюдения с модельными ccf вне пределов интервала от 0 до 1:",
                  description="Обнаружено {0} наблюдений с модельными ccf вне пределов интервала от 0 до 1",
                  request=lambda table: table.filter((f.col('ccf_longrun') < 0) & (f.col('ccf_longrun') > 1)).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt26)

    qt27 = QATest(
        name=("Наблюдения с незаполненными модельными ccf и флагом дефолта равным 0 для внебалансовых" +
              " продуктов либо наоборот:"),
        description=("Обнаружено {0} наблюдений с незаполненными модельными ccf и флагом дефолта равным" +
                     " 0 для внебалансовых продуктов либо наоборот"),
        request=lambda table: table.filter((((f.col('ccf_longrun').isNull()) & (f.col('cur_default') == 0)) |
                                            ((f.col('ccf_longrun').isNotNull()) & (f.col('cur_default') == 1))) &
                                           (f.col('cred_type').isin(['ВКЛ', 'ОВ']))).count(),
        conditions=lambda x: "red" if x > 0 else "green",
        table=portfolio)
    portfolio_qa.add_test(qt27)

    qt28 = QATest(name="Просроченный долг без дней просрочки",
                  description="Обнаружено {0} наблюдений с просроченным долгом с 0 днями просрочки",
                  request=lambda table: table.filter((f.col("debt_ovr_rub") > 0) & (f.col("delinq_days") == 0)).count(),
                  conditions=lambda x: "red" if ((x >= 2) or (x < 0)) else "yellow" if x > 1 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt28)

    qt29 = QATest(
        name=("Наблюдения с незаполненными модельными ccf с эффектом downturn и флагом дефолта равным 0" +
              " для внебалансовых продуктов либо наоборот:"),
        description=("Обнаружено {0} наблюдений с незаполненными модельными ccf с эффектом downturn и флагом" +
                     " дефолта равным 0 для внебалансовых продуктов либо наоборот"),
        request=lambda table: table.filter((((f.col('ccf_downturn').isNull()) & (f.col('cur_default') == 0)) |
                                            ((f.col('ccf_downturn').isNotNull()) & (f.col('cur_default') == 1))) &
                                           (f.col('cred_type').isin(['ВКЛ', 'ОВ']))).count(),
        conditions=lambda x: "red" if x > 0 else "green",
        table=portfolio)
    portfolio_qa.add_test(qt29)

    qt30 = QATest(name="Наблюдения с модельными ccf с эффектом downturn меньше, чем модельные ccf:",
                  description=("Обнаружено {0} наблюдений с модельными ccf с эффектом downturn меньше," +
                               " чем модельные ccf"),
                  request=lambda table: table.filter((f.col('ccf_downturn') < f.col('ccf_longrun'))).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt30)

    qt31 = QATest(name="Наблюдения с незаполненными продуктом:",
                  description="Обнаружено {0} наблюдений с незаполненными продуктом",
                  request=lambda table: table.filter((f.col('product').isNull())).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt31)

    qt32 = QATest(name="Наблюдения с незаполненными типом кредита:",
                  description="Обнаружено {0} наблюдений с незаполненными типом кредита",
                  request=lambda table: table.filter((f.col('cred_type').isNull())).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt32)

    qt33 = QATest(name="Наблюдения с ненулевым внебалансом для разовых кредитов:",
                  description="Обнаружено {0} наблюдений с ненулевым внебалансом для разовых кредитов",
                  request=lambda table: table.filter(
                      (f.col('off_balance_at_report_dt') > 0) & (f.col('cred_type') == 'РК')).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt33)

    qt34 = QATest(name="Наблюдения с внебалансом меньшим 0:",
                  description="Обнаружено {0} наблюдений с внебалансом меньшим 0",
                  request=lambda table: table.filter((f.col('off_balance_at_report_dt') < 0)).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt34)

    qt35 = QATest(name="Наблюдения с незаполненной отчетной датой:",
                  description="Обнаружено {0} наблюдений с незаполненной отчетной датой",
                  request=lambda table: table.filter((f.col('report_dt').isNull())).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt35)

    qt37 = QATest(name="Наблюдения с незаполненным id кредита:",
                  description="Обнаружено {0} наблюдений с незаполненным id кредита",
                  request=lambda table: table.filter((f.col('agr_cred_id').isNull())).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt37)

    qt38 = QATest(name="Наблюдения с незаполненным инн клиента:",
                  description="Обнаружено {0} наблюдений с незаполненным инн клиента",
                  request=lambda table: table.filter((f.col('inn').isNull())).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt38)

    qt39 = QATest(name="Наблюдения с незаполненной датой подписания клиента:",
                  description="Обнаружено {0} наблюдений с незаполненной датой подписания клиента",
                  request=lambda table: table.filter((f.col('signed_dt').isNull())).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt39)

    qt40 = QATest(name="Наблюдения с датой реструктуризации раньшей, чем дата подписания клиента:",
                  description="Обнаружено {0} наблюдений с датой реструктуризации раньшей, чем дата подписания клиента",
                  request=lambda table: table.filter(f.col('restruct_dt') < f.col('signed_dt')).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt40)

    qt41 = QATest(name="Наблюдения с датой продажи раньшей, чем дата подписания клиента:",
                  description="Обнаружено {0} наблюдений с датой продажи раньшей, чем дата подписания клиента",
                  request=lambda table: table.filter(f.col('sell_dt') < f.col('signed_dt')).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt41)

    qt42 = QATest(name="Наблюдения с датой списания раньшей, чем дата подписания клиента:",
                  description="Обнаружено {0} наблюдений с датой списания раньшей, чем дата подписания клиента",
                  request=lambda table: table.filter(f.col('writeoff_dt') < f.col('signed_dt')).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt42)

    qt43 = QATest(name="Наблюдения с pd rwa вне пределов интервала от 0 до 1:",
                  description="Обнаружено {0} наблюдений с pd rwa вне пределов интервала от 0 до 1",
                  request=lambda table: table.filter((f.col('pd_rwa') > 1) | (f.col('pd_rwa') < 0)).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt43)

    qt44 = QATest(name="Наблюдения с pd rwa равным 1 и флагом дефолта равным 0 либо наоборот:",
                  description="Обнаружено {0} наблюдений с pd rwa равным 1 и флагом дефолта равным 0 либо наоборот",
                  request=lambda table: table.filter(((f.col('pd_rwa') == 1) & (f.col('cur_default') == 0)) |
                                                     ((f.col('pd_rwa') != 1) & (f.col('cur_default') == 1))).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt44)

    qt45 = QATest(name="Наблюдения с незаполненной валютой кредита:",
                  description="Обнаружено {0} наблюдений с незаполненной валютой кредита",
                  request=lambda table: table.filter((f.col('currency').isNull())).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt45)

    qt46 = QATest(name="Наблюдения с незаполненным рейтингом ttc:",
                  description="Обнаружено {0} наблюдений с незаполненным рейтингом ttc",
                  request=lambda table: table.filter((f.col('rating_ttc').isNull())).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt46)

    qt47 = QATest(name="Наблюдения с незаполненным рейтингом pit:",
                  description="Обнаружено {0} наблюдений с незаполненным рейтингом pit",
                  request=lambda table: table.filter((f.col('rating_pit').isNull())).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt47)

    qt48 = QATest(name="Наблюдения с рейтингом ttc равным 26 и флагом дефолта равным 0 либо наоборот:",
                  description=("Обнаружено {0} наблюдений с рейтингом ttc равным 26 и флагом дефолта равным 0" +
                               " либо наоборот"),
                  request=lambda table: table.filter(((f.col('rating_ttc') == 26) & (f.col('cur_default') == 0)) |
                                                     ((f.col('rating_ttc') != 26) & (
                                                             f.col('cur_default') == 1))).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt48)

    qt49 = QATest(name="Наблюдения с рейтингом pit равным 26 и флагом дефолта равным 0 либо наоборот:",
                  description=("Обнаружено {0} наблюдений с рейтингом pit равным 26 и флагом дефолта равным" +
                               " 0 либо наоборот"),
                  request=lambda table: table.filter(((f.col('rating_pit') == 26) & (f.col('cur_default') == 0)) |
                                                     ((f.col('rating_pit') != 26) & (
                                                             f.col('cur_default') == 1))).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt49)

    qt50 = QATest(name="Наблюдения со ставкой дисконтирования вне пределов интервала от 0 до 0.5:",
                  description="Обнаружено {0} наблюдений со ставкой дисконтирования вне пределов интервала от 0 до 0.5",
                  request=lambda table: table.filter(((f.col('disc_rate') < 0) & (f.col('disc_rate') > 0.5))).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt50)

    qt51 = QATest(
        name=("Наблюдения с незаполненной датой дефолта в течение 12 месяцев и заполенным флагом дефолта" +
              " в течение 12 месяцев либо наоборот:"),
        description=("Обнаружено {0} наблюдений с незаполненной датой дефолта в течение 12 месяцев и" +
                     " заполенным флагом дефолта в течение 12 месяцев либо наоборот"),
        request=lambda table: table.filter(
            ((f.col('default_in_1y_dt').isNull()) & (f.col('default_in_1y_flag').isNotNull())) |
            ((f.col('default_in_1y_dt').isNotNull()) & (f.col('default_in_1y_flag').isNull()))).count(),
        conditions=lambda x: "red" if x > 0 else "green",
        table=portfolio)
    portfolio_qa.add_test(qt51)

    qt52 = QATest(name="Наблюдения с отрицательным значением входного кэш флоу:",
                  description="Обнаружено {0} наблюдений с отрицательным значением входного кэш флоу",
                  request=lambda table: table.filter(f.col('cash_flow') < 0).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt52)

    qt53 = QATest(name="Наблюдения с отрицательным значением дисконтированного входного кэш флоу:",
                  description="Обнаружено {0} наблюдений с отрицательным значением дисконтированного входного кэш флоу",
                  request=lambda table: table.filter(f.col('cash_flow_disc') < 0).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt53)

    qt54 = QATest(name="Наблюдения с положительным значением входного кэш флоу и нулевым изменением баланса:",
                  description=("Обнаружено {0} наблюдений с положительным значением " +
                               "входного кэш флоу и нулевым изменением баланса"),
                  request=lambda table: table.withColumn('lag_balance_at_report_dt',
                                                         f.lag(f.col('balance_at_report_dt'), 1, default=None) \
                                                         .over(Window.partitionBy('agr_cred_id').orderBy(
                                                             'report_dt'))).filter(
                      (f.col('balance_at_report_dt') - f.col('lag_balance_at_report_dt') == 0) & (
                              f.col('cash_flow') > 0)).count(),
                  conditions=lambda x: "red" if x > 0 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt54)

    qt55 = QATest(
        name="Наблюдения с положительным значением дисконтированного входного кэш флоу и нулевым изменением баланса:",
        description=("Обнаружено {0} наблюдений с  положительным значением дисконтированного" +
                     " входного кэш флоу и нулевым изменением баланса"),
        request=lambda table: table.withColumn('lag_balance_at_report_dt',
                                               f.lag(f.col('balance_at_report_dt'), 1, default=None) \
                                               .over(Window.partitionBy('agr_cred_id').orderBy('report_dt'))).filter(
            (f.col('balance_at_report_dt') - f.col('lag_balance_at_report_dt') == 0) & (
                    f.col('cash_flow_disc') > 0)).count(),
        conditions=lambda x: "red" if x > 0 else "green",
        table=portfolio)
    portfolio_qa.add_test(qt55)

    def __months_after_creation(table):
        table_signature = table.select("TABLE_SIGNATURE").limit(1).collect()[0][0]
        signature_date = re.search(r"\d{4}-\d{2}-\d{2}", table_signature).group(0)
        months_after_creation = (datetime.now() - datetime.strptime(signature_date,
                                                                    "%Y-%m-%d")).total_seconds() / 2628288.0
        return round(months_after_creation, 2)

    qt56 = QATest(name="Время от создания таблицы",
                  description="Таблица была создана {0} месяцев назад",
                  request=lambda table: __months_after_creation(table),
                  conditions=lambda x: "red" if ((x >= 2) or (x < 0)) else "yellow" if x > 1 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt56)

    def __months_after_last_report(table):
        max_report_dt_str = table.select(f.max("report_dt")).collect()[0][0]
        max_report_dt = datetime.strptime(max_report_dt_str, "%Y-%m-%d")
        months_after_last_report = (datetime.now() - max_report_dt).total_seconds() / 2628288.0
        return round(months_after_last_report, 2)

    qt57 = QATest(name="Последняя отчётная дата",
                  description="От последней отчёной даты (report_dt) прошло {0} месяцев",
                  request=lambda table: __months_after_last_report(table),
                  conditions=lambda x: "red" if ((x >= 2) or (x < 0)) else "yellow" if x > 1 else "green",
                  table=portfolio)
    portfolio_qa.add_test(qt57)

    qt58 = QATest(
        name='Наблюдения с флагом "Дефолт в течение года", но без флага "Сейчас в дефолте" в течение 12 месяцев',
        description=('Обнаружено {0} наблюдений с флагом "Дефолт в течение года", ' +
                     ' но без флага "Сейчас в дефолте" в течение 12 месяцев'),
        request=lambda table: table.filter(f.col("default_in_1y_flag") == 1).alias("a") \
            .join(table.alias("b"),
                  on=["agr_cred_id"], how="inner") \
            .filter((f.months_between("b.report_dt", "a.report_dt") <= 12) &
                    (f.months_between("b.report_dt", "a.report_dt") > 0)) \
            .groupby(["a.agr_cred_id", "a.report_dt"]) \
            .agg(f.max("b.cur_default").alias("cur_default_in_feature"), f.count("b.cur_default").alias("mip")) \
            .filter((f.col("cur_default_in_feature") == 0) & (f.col("mip") > 11)) \
            .count(),
        conditions=lambda x: "red" if x > 0 else "green",
        table=portfolio)
    portfolio_qa.add_test(qt58)
    
    qt59 = QATest(
        name='Наблюдения с аномальным приростом дней просрочки',
        description=('Обнаружено {0} наблюдений, в которых прирост количества дней просрочки превышает количество дней, ' +
                     'прошедших с предыдущей даты отчёта'),
        request=lambda table: table\
                .withColumn("prev_report_dt", f.lag(f.col("report_dt"), ).over(Window.partitionBy("agr_cred_id").orderBy("report_dt")))\
                .withColumn("prev_delinq_days", f.lag(f.col("delinq_days"), ).over(Window.partitionBy("agr_cred_id").orderBy("report_dt")))\
                .withColumn("days_passed", f.datediff(f.to_date(f.col("report_dt")), f.to_date(f.col("prev_report_dt"))))\
                .withColumn("delinq_got", f.col("delinq_days") - f.col("prev_delinq_days"))  \
                .filter(f.col("delinq_got")>f.col("days_passed"))\
                .count(),
        conditions=lambda x: "red" if x > 0 else "green",
        table=portfolio)
    portfolio_qa.add_test(qt59)
    
    return portfolio_qa

def get_issues_dq_tests(issues):
    issues_qa = QualityAssessor()
    # Добавляем тесты в ассессор
    qt1 = QATest(name="Выдачи в дефолте",
                 description="Обнаружено {0} выдач в дефолте",
                 request=lambda table: table.filter(f.col("cur_default") == 1).count(),
                 conditions=lambda x: "yellow" if x > 0 else "green",
                 table=issues)
    issues_qa.add_test(qt1)

    return issues_qa
