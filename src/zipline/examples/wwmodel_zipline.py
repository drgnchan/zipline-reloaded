import datetime
import logging as log

import jqdatasdk as jqdata
import numpy as np
import pandas as pd
import zipline as zp
from scipy.stats import norm
from zipline.api import order_target, symbol
from zipline import run_algorithm
from zipline.finance import commission, slippage
from pandas import Timestamp


def initialize(context):
    set_options(context)
    set_params(context)


# 设置期权参数
def set_options(context):
    context.securities = '002156.XSHE'  # 回测标的
    context.securities_x = '002156'  # 回测标的
    context.K = 1
    context.T = 30  # 合约期限
    context.rf = 0.09  # 无风险利率
    context.sigma = volatility(context)
    context.S0 = secinitialprice(context)
    context.startdate = context.sim_params.start_session.date()
    context.maturity = calculate_maturity(context)
    context.NP = context.portfolio.cash / 1  # 名义本金
    context.YearDay = float(365)
    context.secname = jqdata.get_security_info(context.securities).display_name
    print('##################################基本信息####################################')
    print(f'标的代码->{context.securities},标的名称->{context.secname},名义本金->{context.NP}')
    print(f'起始日期->{context.startdate},到期日期->{context.maturity}')
    print(
        f'期初价格->{context.S0},执行价格->{context.K},合约期限->{context.T},无风险利率->{context.rf},波动率->{round(context.sigma, 2)}')
    print('##############################################################################')


# 设置参数
def set_params(context):
    # set_order_cost(OrderCost(close_tax=0.002, open_commission=0.0003, \
    #                          close_commission=0.0003, min_commission=5), type='stock')
    # set_option('use_real_price', True)
    # zp.api.set_benchmark(context.securities_x)
    zp.api.set_slippage(zp.finance.slippage.FixedSlippage(0))  # 滑点


def before_trading_start(context, data):
    # 获取当日日期
    current_date = context.datetime.date()
    # 第一天建仓
    if current_date == context.startdate:
        delta = DeltaCalculator(context, context.S0)
        DeltaPosition = round(context.NP / context.S0 * delta / 100) * 100
        context.lastdelta = delta
        order_target(symbol((context.securities_x)), DeltaPosition)
    # 第二天到倒数第二天判断分红拆股
    if current_date > context.startdate and current_date < context.maturity:
        checkadj = jqdata.get_price(context.securities, end_date=current_date, \
                                    fields=['close', 'factor'], fq='pre', count=2)
        if checkadj.iloc[0, 1] != checkadj.iloc[1, 1]:
            temp = context.S0
            context.S0 = round(context.S0 * checkadj.iloc[0, 1] / checkadj.iloc[1, 1], 2)
            print(f'期初价格调整{temp}-->{context.S0}')
    # 到期日清仓
    if current_date == context.maturity:
        order_target(symbol(context.securities_x), 0)
        print(f'今天清仓日->{str(current_date)}')
    # 到期日之后就没事了
    if current_date > context.maturity:
        pass


def handle_data(context, data):
    # 获取时间并判断
    current_time = context.datetime
    current_date = current_time.date()

    # 第一天建仓
    if current_date == context.startdate:
        delta = DeltaCalculator(context, context.S0)
        DeltaPosition = round(context.NP / context.S0 * delta / 100) * 100
        context.lastdelta = delta
        order_target(symbol((context.securities_x)), DeltaPosition)
    # 第二天到倒数第二天判断分红拆股
    if current_date > context.startdate and current_date < context.maturity:
        checkadj = jqdata.get_price(context.securities, end_date=current_date, fields=['close', 'factor'], fq='pre',
                                    count=2)
        if checkadj.iloc[0, 1] != checkadj.iloc[1, 1]:
            temp = context.S0
            context.S0 = round(context.S0 * checkadj.iloc[0, 1] / checkadj.iloc[1, 1], 2)
            print(f'期初价格调整{temp}-->{context.S0}')
    # 到期日清仓
    if current_date == context.maturity:
        order_target(symbol(context.securities_x), 0)
        log.info('今天清仓日->', str(current_date))
    # 到期日之后就没事了
    if current_date > context.maturity:
        pass

    # 只有第二天到倒数第二天才会有对冲
    # 9点30会取到昨天的收盘价,所以我们跳过这一分钟。
    if current_date > context.startdate and current_date < context.maturity and \
            (current_time.hour > 9 or current_time.minute > 30):

        # 中间交易日的触发对冲条件
        price = jqdata.get_price(context.securities, count=1, end_date=current_date, frequency='minute', fields='close',
                                 skip_paused=False, fq='pre')
        currentdelta = DeltaCalculator(context, price.iloc[0, 0])
        threshold = WhalleyWilmottThreshold(context, price.iloc[0, 0])

        if abs(context.lastdelta - currentdelta) > threshold:
            print(f'delta之差绝对值超过B[{threshold}]')
            delta = DeltaCalculator(context, price.iloc[0, 0])
            DeltaPosition = round(context.NP / context.S0 * delta / 100) * 100
            context.lastdelta = delta
            order_target(symbol(context.securities_x), DeltaPosition)


# 波动率
def volatility(context):
    end_date: Timestamp = context.datetime
    start_date = end_date.date() - datetime.timedelta(days=365)
    price = jqdata.get_price(context.securities, start_date=start_date, end_date=end_date, frequency='daily',
                             fields='close',
                             skip_paused=False, fq='pre')
    rets = np.diff(np.log(price), axis=0)
    std = rets.std() * np.sqrt(250)
    return std


# 价格获取
def secinitialprice(context):
    start_date = context.datetime.date()
    S0 = jqdata.get_price(context.securities, start_date=start_date, end_date=start_date, frequency='daily',
                          fields='open', fq=None)
    return S0.values[0][0]


# 日期
def calculate_maturity(context):
    matday = context.startdate + datetime.timedelta(days=context.T)
    array = jqdata.get_all_trade_days()
    index = np.where(array >= matday)[0][0]  # datetime.date(matday.year,matday.month,matday.day)
    truematday = array[index]
    return truematday


# Delta
def DeltaCalculator(context, S):
    current_date = context.datetime.date()
    Tau = (context.maturity - current_date).days + 1
    d1 = (np.log(S / (context.S0 * context.K)) + (context.rf + context.sigma ** 2 / 2) * (Tau / context.YearDay)) \
         / (context.sigma * np.sqrt(Tau / context.YearDay))
    delta = norm.cdf(d1)  # 概率密度函数
    return delta


# Gamma
def GammaCalculator(context, S):
    current_date = context.datetime.date()
    Tau = (context.maturity - current_date).days + 1
    d1 = (np.log(S / (context.S0 * context.K)) + (context.rf + context.sigma ** 2 / 2) * (Tau / context.YearDay)) \
         / (context.sigma * np.sqrt(Tau / context.YearDay))
    gamma = norm.pdf(d1) / (S * context.sigma * np.sqrt(Tau / context.YearDay))
    return gamma


# 风险参数
def WhalleyWilmottThreshold(context, S):
    current_date = context.datetime.date()
    risktolerance = 5
    tradingcost = 0.00055
    Tau = (context.maturity - current_date).days + 1
    gamma = GammaCalculator(context, S)
    a = np.exp(-context.rf * Tau / context.YearDay) * tradingcost * S * gamma ** 2
    wwt = (3.0 / 2.0 * a / risktolerance) ** (1.0 / 3.0)
    return wwt


jqdata.auth("17316886801", '660615crL')

from zipline.data.bundles import register
from zipline.data.bundles.csvdir import csvdir_equities

start_session = pd.Timestamp('2021-1-4')
end_session = pd.Timestamp('2022-8-5')
register(
    'a_stock',
    calendar_name='XSHG'
)
from zipline.utils.calendar_utils import get_calendar

result = run_algorithm(
    start=datetime.datetime(2022, 7, 5),
    end=datetime.datetime(2022, 8, 5),
    initialize=initialize,
    handle_data=handle_data,
    capital_base=2000000,
    bundle='a_stock',
    data_frequency='daily',
    trading_calendar=get_calendar('XSHG')
)

print(result)

result.to_csv('sjafjdhfj.csv')
